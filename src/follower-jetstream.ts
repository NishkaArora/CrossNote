// Followers-only Jetstream source for the CrossNote pipeline.
//
// Jetstream only sends posts by accounts that follow the labeller. Each
// original or quote post (no replies; reposts are a different collection)
// whose language tags include English, or that has no language tag, goes to
// the Python matcher (pipeline/crossnote_server.py: length filter -> TF-IDF
// gate -> embedding gate -> top 3 -> gpt-6.1-sol). On a match it applies the
// misinformation label and replies with the full note (src/reply.ts).
//
// CROSSNOTE_DRY_RUN is on unless set to "0": matches are logged, nothing is
// labelled or posted. Enabled with AUTO_LABEL_SCOPE=followers; see index.ts.
import { Jetstream } from "@skyware/jetstream";
import { appendFileSync } from "fs";
import { LabelerServer } from "./labeler-server.js";
import { matchPost, throttled, type MatchResponse } from "./crossnote-pipeline.js";
import { buildPosts, postReplyThread } from "./reply.js";
import * as db from "./db.js";

const LABEL_VALUE         = "misinformation";
const DRY_RUN             = process.env.CROSSNOTE_DRY_RUN !== "0";
const MATCH_LOG_PATH      = process.env.MATCH_LOG_PATH ?? "/data/crossnote_matches.jsonl";
const RATE_CSV_PATH       = "/data/follower_jetstream_rate.csv";
const PUBLIC_APPVIEW      = "https://public.api.bsky.app";
const JETSTREAM_MAX_DIDS  = 10_000;  // Jetstream's wantedDids limit
const FOLLOWER_REFRESH_MS = parseInt(process.env.FOLLOWER_REFRESH_SECONDS ?? "300", 10) * 1000;
const STARTUP_RETRY_MS    = 30_000;
const MAX_REPLAY_US       = 3600 * 1e6;  // ignore saved cursors older than 1h

// ── Followers ────────────────────────────────────────────────────────────────

/** DIDs of every account that follows `actor`, from the public AppView (no auth). */
async function fetchFollowers(actor: string): Promise<Set<string>> {
  const dids = new Set<string>();
  let cursor: string | undefined;
  do {
    const url = new URL(`${PUBLIC_APPVIEW}/xrpc/app.bsky.graph.getFollowers`);
    url.searchParams.set("actor", actor);
    url.searchParams.set("limit", "100");
    if (cursor) url.searchParams.set("cursor", cursor);

    const res = await fetch(url, { signal: AbortSignal.timeout(30_000) });
    if (!res.ok) throw new Error(`getFollowers → ${res.status}: ${(await res.text()).slice(0, 200)}`);
    const body = (await res.json()) as { followers: { did: string }[]; cursor?: string };

    for (const f of body.followers) dids.add(f.did);
    cursor = body.followers.length > 0 ? body.cursor : undefined;
  } while (cursor);
  return dids;
}

/**
 * Initial follower list, retried until it succeeds and is non-empty.
 * Never returns an empty set: Jetstream treats an empty wantedDids as "every
 * account", which would silently turn this into the full stream.
 */
async function initialFollowers(did: string): Promise<Set<string>> {
  for (;;) {
    try {
      const followers = await fetchFollowers(did);
      if (followers.size > 0) return followers;
      console.warn(`[followers] no visible followers; retrying in ${STARTUP_RETRY_MS / 1000}s`);
    } catch (e) {
      console.error("[followers] fetch failed:", e instanceof Error ? e.message : e,
                    `— retrying in ${STARTUP_RETRY_MS / 1000}s`);
    }
    await new Promise((r) => setTimeout(r, STARTUP_RETRY_MS));
  }
}

function sameSet(a: Set<string>, b: Set<string>): boolean {
  if (a.size !== b.size) return false;
  for (const x of a) if (!b.has(x)) return false;
  return true;
}

function capDids(followers: Set<string>): string[] {
  const dids = [...followers];
  if (dids.length > JETSTREAM_MAX_DIDS) {
    console.error(`[followers] ${dids.length} followers exceeds Jetstream's ${JETSTREAM_MAX_DIDS} DID limit; ` +
                  `only the first ${JETSTREAM_MAX_DIDS} are watched`);
    return dids.slice(0, JETSTREAM_MAX_DIDS);
  }
  return dids;
}

// ── Stats ────────────────────────────────────────────────────────────────────

export interface FollowerJetstreamStats {
  processed: number;   // follower posts received
  skipped: number;     // replies, non-English, no text, already labelled
  matched: number;     // Stage 3 found an applicable note
  labeled: number;     // label applied (0 in dry run)
  replied: number;     // reply thread posted (0 in dry run)
  errors: number;
  throttled: number;
  postsPerSec: number;
  followers: number;
  dryRun: boolean;
  actionsDisabled: boolean;
}

export let followerStats: FollowerJetstreamStats = {
  processed: 0, skipped: 0, matched: 0, labeled: 0, replied: 0, errors: 0, throttled: 0,
  postsPerSec: 0, followers: 0, dryRun: DRY_RUN, actionsDisabled: false,
};

// ── Post filters ─────────────────────────────────────────────────────────────

interface PostRecord {
  text?: string;
  reply?: unknown;
  langs?: string[];
}

/** No language tag -> keep; otherwise keep if any tag is English (en, en-US, ...). */
function isEnglishOrUntagged(langs: string[] | undefined): boolean {
  if (!Array.isArray(langs) || langs.length === 0) return true;
  return langs.some((l) => /^en(-|$)/i.test(l));
}

function logMatch(entry: Record<string, unknown>): void {
  try {
    appendFileSync(MATCH_LOG_PATH, JSON.stringify(entry) + "\n");
  } catch (e) {
    console.error("Failed to write match log:", e instanceof Error ? e.message : e);
  }
}

/** An account takedown/suspension: stop labelling and replying for good. */
function isAccountDown(e: unknown): boolean {
  const msg = e instanceof Error ? `${e.name} ${e.message}` : String(e);
  const status = (e as { status?: number })?.status;
  return /AccountTakedown|AccountDeactivated|AccountSuspended/i.test(msg) || status === 401 || status === 403;
}

// ── Stream ───────────────────────────────────────────────────────────────────

export async function startFollowerJetstream(labeler: LabelerServer): Promise<void> {
  const ownDid = process.env.LABELER_DID!;
  console.log(DRY_RUN
    ? "[crossnote] DRY RUN: matches are logged only (set CROSSNOTE_DRY_RUN=0 to label and reply)"
    : "[crossnote] LIVE: matches are labelled and replied to");

  let followers = await initialFollowers(ownDid);
  followerStats = { ...followerStats, followers: followers.size };
  console.log(`[followers] watching ${followers.size} followers of ${ownDid}`);

  // Don't resume from a stale cursor (e.g. one left by an earlier full-stream
  // run): replaying days of follower posts would label and reply to a backlog
  // of old posts in one burst.
  let cursor = db.getCursor();
  if (cursor && Date.now() * 1000 - cursor > MAX_REPLAY_US) {
    console.log("[followers] saved cursor is older than 1h; starting from now");
    cursor = undefined;
  }

  const jetstream = new Jetstream({
    wantedCollections: ["app.bsky.feed.post"],
    wantedDids: capDids(followers),
    cursor,
  });

  let processed = 0, skipped = 0, matched = 0, labeled = 0, replied = 0, errors = 0;
  let actionsDisabled = false;
  let intervalPosts = 0;
  let intervalStart = Date.now();
  const seen = new Set<string>();  // dry run never writes the labels table

  try {
    appendFileSync(RATE_CSV_PATH,
      "timestamp,posts_per_sec,processed,skipped,matched,labeled,replied,errors,throttled,followers\n", { flag: "ax" });
  } catch {}

  setInterval(() => {
    const elapsed = (Date.now() - intervalStart) / 1000;
    const postsPerSec = parseFloat((intervalPosts / elapsed).toFixed(2));
    const ts = new Date().toISOString();

    console.log(`Stats: ${processed} processed, ${skipped} skipped, ${matched} matched, ${labeled} labeled, ` +
                `${replied} replied, ${errors} errors, ${throttled} throttled, ${followers.size} followers` +
                (DRY_RUN ? " [dry run]" : "") + (actionsDisabled ? " [ACTIONS DISABLED]" : ""));
    try {
      appendFileSync(RATE_CSV_PATH,
        `${ts},${postsPerSec},${processed},${skipped},${matched},${labeled},${replied},${errors},${throttled},${followers.size}\n`);
    } catch (e) {
      console.error("Failed to write rate CSV:", e instanceof Error ? e.message : e);
    }

    followerStats = { processed, skipped, matched, labeled, replied, errors, throttled, postsPerSec,
                      followers: followers.size, dryRun: DRY_RUN, actionsDisabled };
    intervalPosts = 0;
    intervalStart = Date.now();
  }, 60_000);

  // Pick up new followers / drop unfollowers without reconnecting.
  setInterval(async () => {
    try {
      const next = await fetchFollowers(ownDid);
      if (next.size === 0) {
        // Never send an empty wantedDids: Jetstream would switch to every account.
        console.warn("[followers] refresh returned no followers; keeping the previous list");
        return;
      }
      if (sameSet(next, followers)) return;
      const added   = [...next].filter((d) => !followers.has(d)).length;
      const removed = [...followers].filter((d) => !next.has(d)).length;
      followers = next;
      followerStats = { ...followerStats, followers: next.size };
      jetstream.updateOptions({ wantedDids: capDids(next) });
      console.log(`[followers] now ${next.size} (+${added} / -${removed})`);
    } catch (e) {
      console.error("[followers] refresh failed:", e instanceof Error ? e.message : e);
    }
  }, FOLLOWER_REFRESH_MS);

  jetstream.on("commit", async (event) => {
    if (event.commit.operation !== "create") return;
    if (event.did === ownDid) return;
    // wantedDids already filters server-side; this also covers the moment
    // before an options update takes effect.
    if (!followers.has(event.did)) return;

    processed++;
    intervalPosts++;
    db.setCursor(event.time_us);  // few posts, so save every one

    // Original posts and quote posts only; image posts are judged on their
    // text, quote posts on the quoting post's own text.
    const record = event.commit.record as PostRecord;
    const uri = `at://${event.did}/${event.commit.collection}/${event.commit.rkey}`;
    const text = record.text?.trim() ?? "";
    if (!text || record.reply || !isEnglishOrUntagged(record.langs) || seen.has(uri) || db.hasLabel(uri)) {
      skipped++;
      return;
    }
    seen.add(uri);

    let result: MatchResponse | null;
    try {
      result = await matchPost(text, uri);
    } catch (e) {
      errors++;
      if (errors % 50 === 1) console.error("Matcher error:", e instanceof Error ? e.message : e);
      return;
    }
    if (!result) return;  // throttled
    if (result.stage === "error" || result.stage === "not_ready") {
      errors++;
      if (errors % 50 === 1) console.error(`Matcher ${result.stage}:`, result.error);
      return;
    }
    const m = result.match;
    if (!m) return;

    matched++;
    const entry: Record<string, unknown> = {
      ts: new Date().toISOString(),
      uri,
      cid: event.commit.cid,
      author: event.did,
      post_text: text,
      note_id: m.note_id,
      tweet_id: m.tweet_id,
      rank: m.rank,
      cosine: m.cosine,
      cosine_max: result.cosine_max,
      tfidf: m.tfidf,
      tfidf_max: result.tfidf_max,
      reason: m.reason,
      candidates: result.candidates,
      library: result.library,
      dry_run: DRY_RUN,
      labeled: false,
      reply_uris: [] as string[],
      error: null as string | null,
    };
    console.log(`[MATCH${DRY_RUN ? " dry-run" : ""}] ${uri} ← note ${m.note_id} ` +
                `(cosine ${m.cosine.toFixed(3)}, tfidf ${m.tfidf.toFixed(3)}): ${m.reason}`);

    if (DRY_RUN || actionsDisabled || !event.commit.cid) {
      if (actionsDisabled) entry.error = "actions disabled after an account error";
      logMatch(entry);
      return;
    }

    try {
      await labeler.emitLabel(uri, LABEL_VALUE, event.commit.cid);
      db.logLabel(uri, event.commit.cid, LABEL_VALUE, text, `CN ${m.note_id}: ${m.reason ?? ""}`);
      entry.labeled = true;
      labeled++;

      entry.reply_uris = await postReplyThread(uri, event.commit.cid, buildPosts(m.reply_note_text, m.tweet_id));
      replied++;
    } catch (e) {
      errors++;
      entry.error = e instanceof Error ? e.message : String(e);
      console.error(`Action failed for ${uri}:`, entry.error);
      if (isAccountDown(e)) {
        actionsDisabled = true;
        console.error("[crossnote] ACCOUNT ERROR: labelling and replying are now disabled until restart");
      }
    }
    logMatch(entry);
  });

  jetstream.on("error", (error) => {
    console.error("Jetstream error:", error);
  });

  jetstream.start();
  console.log(`Jetstream connected, watching posts by ${followers.size} followers...`);
}

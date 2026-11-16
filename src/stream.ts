// Jetstream source for the CrossNote pipeline, in one of two modes:
//
//   followers  (default) Jetstream only sends posts by accounts that follow
//              the labeller; the follower list is refreshed every
//              FOLLOWER_REFRESH_SECONDS.
//   full       every post on Bluesky. Replies are capped at
//              MAX_REPLIES_PER_HOUR; matches over the cap are labelled and
//              logged but not replied to.
//
// Each original or quote post (no replies; reposts are a different
// collection) whose language tags include English, or that has no language
// tag, goes to the Python matcher (pipeline/crossnote_server.py: length
// filter -> TF-IDF gate -> embedding gate -> top 3 -> LLM applicability).
// On a match it applies the misinformation label and replies with the full
// note (src/reply.ts).
//
// CROSSNOTE_DRY_RUN is on unless set to "0": matches are logged, nothing is
// labelled or posted.
import { Jetstream } from "@skyware/jetstream";
import { appendFileSync } from "fs";
import { join } from "path";
import { LabelerServer } from "./labeler-server.js";
import { matchPost, throttled, type MatchResponse } from "./crossnote-pipeline.js";
import { buildPosts, postReplyThread } from "./reply.js";
import * as db from "./db.js";

export type StreamMode = "followers" | "full";

const LABEL_VALUE         = "misinformation";
const DRY_RUN             = process.env.CROSSNOTE_DRY_RUN !== "0";
const DATA_DIR            = process.env.DATA_DIR ?? "/data";
const MATCH_LOG_PATH      = process.env.MATCH_LOG_PATH ?? join(DATA_DIR, "crossnote_matches.jsonl");
const RATE_CSV_PATH       = process.env.RATE_CSV_PATH ?? join(DATA_DIR, "stream_rate.csv");
const PUBLIC_APPVIEW      = "https://public.api.bsky.app";
const JETSTREAM_MAX_DIDS  = 10_000;  // Jetstream's wantedDids limit
const FOLLOWER_REFRESH_MS = parseInt(process.env.FOLLOWER_REFRESH_SECONDS ?? "300", 10) * 1000;
const MAX_REPLIES_PER_HOUR = parseInt(process.env.MAX_REPLIES_PER_HOUR ?? "20", 10);
// Accounts that asked to be excluded: their posts are dropped before any
// processing or logging, in every mode.
const EXCLUDED_DIDS = new Set(
  (process.env.EXCLUDED_DIDS ?? "").split(",").map((d) => d.trim()).filter(Boolean));
const STARTUP_RETRY_MS    = 30_000;
const MAX_REPLAY_US       = 3600 * 1e6;  // ignore saved cursors older than 1h
const CURSOR_SAVE_MS      = 1_000;       // save the cursor at most once a second
const SEEN_MAX            = 50_000;      // bound on the in-memory dedupe set

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
 * account", which would silently turn followers mode into the full stream.
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

export interface StreamStats {
  mode: StreamMode;
  processed: number;   // posts received
  skipped: number;     // replies, non-English, no text, already labelled
  matched: number;     // Stage 3 found an applicable note
  labeled: number;     // label applied (0 in dry run)
  replied: number;     // reply thread posted (0 in dry run)
  replyCapped: number; // matches not replied to because of MAX_REPLIES_PER_HOUR
  errors: number;
  throttled: number;
  postsPerSec: number;
  followers: number;   // 0 in full mode
  dryRun: boolean;
  actionsDisabled: boolean;
}

export let streamStats: StreamStats = {
  mode: "followers", processed: 0, skipped: 0, matched: 0, labeled: 0, replied: 0, replyCapped: 0,
  errors: 0, throttled: 0, postsPerSec: 0, followers: 0, dryRun: DRY_RUN, actionsDisabled: false,
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

export async function startStream(labeler: LabelerServer, mode: StreamMode): Promise<void> {
  const ownDid = process.env.LABELER_DID!;
  const full = mode === "full";
  console.log(`[crossnote] mode: ${full ? "FULL FEED (every post)" : "followers only"}`);
  console.log(DRY_RUN
    ? "[crossnote] DRY RUN: matches are logged only (set CROSSNOTE_DRY_RUN=0 to label and reply)"
    : "[crossnote] LIVE: matches are labelled and replied to");
  if (full) console.log(`[crossnote] replies capped at ${MAX_REPLIES_PER_HOUR} per hour`);
  if (EXCLUDED_DIDS.size) console.log(`[crossnote] ${EXCLUDED_DIDS.size} excluded account(s)`);

  let followers = new Set<string>();
  if (!full) {
    followers = await initialFollowers(ownDid);
    console.log(`[followers] watching ${followers.size} followers of ${ownDid}`);
  }
  streamStats = { ...streamStats, mode, followers: followers.size };

  // Don't resume from a stale cursor: replaying a backlog would label and
  // reply to old posts in one burst. Full mode always starts from now, so
  // switching modes never replays an hour of the whole network.
  let cursor = full ? undefined : db.getCursor();
  if (cursor && Date.now() * 1000 - cursor > MAX_REPLAY_US) {
    console.log("[stream] saved cursor is older than 1h; starting from now");
    cursor = undefined;
  }

  const jetstream = new Jetstream({
    wantedCollections: ["app.bsky.feed.post"],
    ...(full ? {} : { wantedDids: capDids(followers) }),
    cursor,
  });

  let processed = 0, skipped = 0, matched = 0, labeled = 0, replied = 0, replyCapped = 0, errors = 0;
  let actionsDisabled = false;
  let intervalPosts = 0;
  let intervalStart = Date.now();
  let lastCursorSave = 0;
  const seen = new Set<string>();       // dry run never writes the labels table
  const replyTimes: number[] = [];      // full mode: reply timestamps in the last hour

  function replyAllowed(): boolean {
    if (!full) return true;
    const hourAgo = Date.now() - 3600_000;
    while (replyTimes.length && replyTimes[0] < hourAgo) replyTimes.shift();
    return replyTimes.length < MAX_REPLIES_PER_HOUR;
  }

  try {
    appendFileSync(RATE_CSV_PATH,
      "timestamp,mode,posts_per_sec,processed,skipped,matched,labeled,replied,reply_capped,errors,throttled,followers\n",
      { flag: "ax" });
  } catch {}

  setInterval(() => {
    const elapsed = (Date.now() - intervalStart) / 1000;
    const postsPerSec = parseFloat((intervalPosts / elapsed).toFixed(2));
    const ts = new Date().toISOString();

    console.log(`Stats [${mode}]: ${processed} processed, ${skipped} skipped, ${matched} matched, ` +
                `${labeled} labeled, ${replied} replied, ${replyCapped} reply-capped, ${errors} errors, ` +
                `${throttled} throttled, ${postsPerSec} posts/sec` +
                (full ? "" : `, ${followers.size} followers`) +
                (DRY_RUN ? " [dry run]" : "") + (actionsDisabled ? " [ACTIONS DISABLED]" : ""));
    try {
      appendFileSync(RATE_CSV_PATH,
        `${ts},${mode},${postsPerSec},${processed},${skipped},${matched},${labeled},${replied},` +
        `${replyCapped},${errors},${throttled},${followers.size}\n`);
    } catch (e) {
      console.error("Failed to write rate CSV:", e instanceof Error ? e.message : e);
    }

    streamStats = { mode, processed, skipped, matched, labeled, replied, replyCapped, errors, throttled,
                    postsPerSec, followers: followers.size, dryRun: DRY_RUN, actionsDisabled };
    intervalPosts = 0;
    intervalStart = Date.now();
  }, 60_000);

  // Followers mode: pick up new followers / drop unfollowers without reconnecting.
  if (!full) {
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
        streamStats = { ...streamStats, followers: next.size };
        jetstream.updateOptions({ wantedDids: capDids(next) });
        console.log(`[followers] now ${next.size} (+${added} / -${removed})`);
      } catch (e) {
        console.error("[followers] refresh failed:", e instanceof Error ? e.message : e);
      }
    }, FOLLOWER_REFRESH_MS);
  }

  jetstream.on("commit", async (event) => {
    if (event.commit.operation !== "create") return;
    if (event.did === ownDid || EXCLUDED_DIDS.has(event.did)) return;
    // wantedDids already filters server-side; this also covers the moment
    // before an options update takes effect.
    if (!full && !followers.has(event.did)) return;

    processed++;
    intervalPosts++;
    const now = Date.now();
    if (now - lastCursorSave >= CURSOR_SAVE_MS) {
      db.setCursor(event.time_us);
      lastCursorSave = now;
    }

    // Original posts and quote posts only; image posts are judged on their
    // text, quote posts on the quoting post's own text.
    const record = event.commit.record as PostRecord;
    const uri = `at://${event.did}/${event.commit.collection}/${event.commit.rkey}`;
    const text = record.text?.trim() ?? "";
    if (!text || record.reply || !isEnglishOrUntagged(record.langs) || seen.has(uri) || db.hasLabel(uri)) {
      skipped++;
      return;
    }
    if (seen.size >= SEEN_MAX) seen.clear();
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
    // No post text or separate author field: the at:// URI (which contains
    // the author's DID) is enough to audit a label, and a deleted post can't
    // be read back from these logs.
    const entry: Record<string, unknown> = {
      ts: new Date().toISOString(),
      mode,
      uri,
      cid: event.commit.cid,
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
      reply_skipped: null as string | null,
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
      db.logLabel(uri, event.commit.cid, LABEL_VALUE, `CN ${m.note_id}: ${m.reason ?? ""}`);
      entry.labeled = true;
      labeled++;

      if (replyAllowed()) {
        replyTimes.push(Date.now());
        entry.reply_uris = await postReplyThread(uri, event.commit.cid, buildPosts(m.reply_note_text, m.tweet_id));
        replied++;
      } else {
        replyCapped++;
        entry.reply_skipped = `over MAX_REPLIES_PER_HOUR (${MAX_REPLIES_PER_HOUR})`;
      }
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
  console.log(full
    ? "Jetstream connected, watching every post..."
    : `Jetstream connected, watching posts by ${followers.size} followers...`);
}

// Read-only API for the demo page (demo/, hosted on GitHub Pages).
//
//   GET  /demo/match?uri=at://...   why one labelled post was matched
//   GET  /demo/summary              aggregate numbers for the front page
//   POST /demo/feedback             "Does this note apply?" answer
//
// Only posts that are labelled right now are served, one at a time by URI:
// there is no endpoint that lists labelled posts or accounts. A post is not
// served if its label was removed, its author is in EXCLUDED_DIDS, the post
// was deleted, or its author asked Bluesky not to show their posts to
// logged-out viewers. All of these return the same 404.
//
// Source data: the match log written by src/stream.ts (scores and Stage 3
// reasons, no post text) and the library snapshot each match was made
// against (note text and timestamps, via the Python matcher).
import express, { type Request } from "express";
import { closeSync, fstatSync, openSync, readSync } from "fs";
import { StringDecoder } from "string_decoder";
import * as db from "./db.js";
import { fetchDemoNotes, fetchMatcherHealth, type DemoNote, type DemoNotes } from "./crossnote-pipeline.js";
import { EXCLUDED_DIDS, MATCH_LOG_PATH, PUBLIC_APPVIEW } from "./stream.js";

// Origins allowed to POST feedback, e.g. https://<user>.github.io. Reads are
// open to any origin: they return nothing beyond what the label and reply
// already make public.
const FEEDBACK_ORIGINS = new Set(
  (process.env.DEMO_ORIGINS ?? "").split(",").map((o) => o.trim().replace(/\/+$/, "")).filter(Boolean));
const FEEDBACK_PER_IP_PER_HOUR = 10;
const FEEDBACK_TOTAL_PER_HOUR  = 300;
const COMMENT_MAX_CHARS        = 1000;
const POST_CACHE_MS            = 10 * 60_000;
const SUMMARY_CACHE_MS         = 10 * 60_000;

const POST_URI = /^at:\/\/(did:[a-z]+:[A-Za-z0-9._:%-]{1,256})\/app\.bsky\.feed\.post\/([A-Za-z0-9._~:-]{1,512})$/;

// ── Match log index ──────────────────────────────────────────────────────────

interface Candidate {
  rank: number;
  note_id: string;
  tweet_id: string;
  cosine: number;
  tfidf: number;
  applicable: boolean;
  reason: string | null;
  error: string | null;
}

interface LoggedMatch extends Candidate {
  ts: string;
  uri: string;
  cosine_max?: number;
  tfidf_max?: number;
  candidates?: Candidate[];
  library: string | null;
  dry_run: boolean;
  labeled: boolean;
  reply_uris?: string[];
}

// Labelled matches by post URI, read incrementally as the log grows.
const matches = new Map<string, LoggedMatch>();
let logOffset = 0;
let logDecoder = new StringDecoder("utf8");
let logPartial = "";

function refreshIndex(): void {
  let fd: number;
  try {
    fd = openSync(MATCH_LOG_PATH, "r");
  } catch {
    return;  // no matches yet
  }
  try {
    const size = fstatSync(fd).size;
    if (size < logOffset) {  // truncated or rewritten (e.g. by scrub_fly_data.py)
      matches.clear();
      logOffset = 0;
      logDecoder = new StringDecoder("utf8");
      logPartial = "";
    }
    const buf = Buffer.alloc(1 << 20);
    while (logOffset < size) {
      const n = readSync(fd, buf, 0, Math.min(buf.length, size - logOffset), logOffset);
      if (n <= 0) break;
      logOffset += n;
      const lines = (logPartial + logDecoder.write(buf.subarray(0, n))).split("\n");
      logPartial = lines.pop() ?? "";
      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          const e = JSON.parse(line) as LoggedMatch;
          if (e.labeled === true && e.dry_run !== true && typeof e.uri === "string") matches.set(e.uri, e);
        } catch {
          // a damaged line; skip it
        }
      }
    }
  } finally {
    closeSync(fd);
  }
}

const didOf = (uri: string) => uri.slice("at://".length).split("/")[0];

/** The logged match for a post that is labelled now, or null. */
function liveMatch(uri: string): LoggedMatch | null {
  refreshIndex();
  const m = matches.get(uri);
  if (!m || EXCLUDED_DIDS.has(didOf(uri)) || !db.hasLabel(uri)) return null;
  return m;
}

function liveMatches(): LoggedMatch[] {
  refreshIndex();
  return [...matches.values()].filter((m) => !EXCLUDED_DIDS.has(didOf(m.uri)) && db.hasLabel(m.uri));
}

// ── Bluesky post check ───────────────────────────────────────────────────────

interface PostInfo { visible: boolean; handle?: string; posted_at?: string }
const postCache = new Map<string, { at: number; info: PostInfo }>();

async function postInfo(uri: string): Promise<PostInfo> {
  const hit = postCache.get(uri);
  if (hit && Date.now() - hit.at < POST_CACHE_MS) return hit.info;

  const url = new URL(`${PUBLIC_APPVIEW}/xrpc/app.bsky.feed.getPosts`);
  url.searchParams.set("uris", uri);
  const res = await fetch(url, { signal: AbortSignal.timeout(10_000) });
  if (!res.ok) throw new Error(`getPosts → ${res.status}`);
  const body = (await res.json()) as {
    posts?: { author?: { handle?: string; labels?: { val?: string }[] }; record?: { createdAt?: string } }[];
  };
  const post = body.posts?.[0];
  const info: PostInfo = !post || post.author?.labels?.some((l) => l.val === "!no-unauthenticated")
    ? { visible: false }
    : { visible: true, handle: post.author?.handle, posted_at: post.record?.createdAt };

  if (postCache.size > 5_000) postCache.clear();
  postCache.set(uri, { at: Date.now(), info });
  return info;
}

/** Creation time encoded in a record key (TID), in ms. */
function tidMillis(rkey: string): number | null {
  const alphabet = "234567abcdefghijklmnopqrstuvwxyz";
  if (!/^[2-7a-j][2-7a-z]{12}$/.test(rkey)) return null;
  let v = 0n;
  for (const ch of rkey) v = v * 32n + BigInt(alphabet.indexOf(ch));
  return Number((v >> 10n) / 1000n);
}

function median(xs: number[]): number | null {
  if (!xs.length) return null;
  const s = [...xs].sort((a, b) => a - b);
  const mid = s.length >> 1;
  return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;
}

// ── Feedback rate limit (in memory) ──────────────────────────────────────────

const feedbackTimes = new Map<string, number[]>();
let feedbackAll: number[] = [];

function feedbackAllowed(ip: string): boolean {
  const hourAgo = Date.now() - 3600_000;
  feedbackAll = feedbackAll.filter((t) => t > hourAgo);
  const mine = (feedbackTimes.get(ip) ?? []).filter((t) => t > hourAgo);
  if (mine.length >= FEEDBACK_PER_IP_PER_HOUR || feedbackAll.length >= FEEDBACK_TOTAL_PER_HOUR) return false;
  mine.push(Date.now());
  feedbackAll.push(Date.now());
  if (feedbackTimes.size > 10_000) feedbackTimes.clear();
  feedbackTimes.set(ip, mine);
  return true;
}

const clientIp = (req: Request) => req.get("fly-client-ip") ?? req.socket.remoteAddress ?? "unknown";

// ── Routes ───────────────────────────────────────────────────────────────────

export const demoRouter = express.Router();

demoRouter.use((req, res, next) => {
  const origin = req.get("origin")?.replace(/\/+$/, "");
  const isFeedback = req.path === "/feedback";
  if (!isFeedback) {
    res.set("Access-Control-Allow-Origin", "*");
  } else if (origin && FEEDBACK_ORIGINS.has(origin)) {
    res.set("Access-Control-Allow-Origin", origin);
    res.set("Vary", "Origin");
    res.set("Access-Control-Allow-Methods", "POST");
    res.set("Access-Control-Allow-Headers", "Content-Type");
    res.set("Access-Control-Max-Age", "86400");
  }
  res.set("Cache-Control", "no-store");
  if (req.method === "OPTIONS") {
    res.sendStatus(204);
    return;
  }
  next();
});

const NOT_FOUND = { error: "NotFound", message: "CrossNote has no current label for this post." };

demoRouter.get("/match", async (req, res) => {
  const uri = typeof req.query.uri === "string" ? req.query.uri : "";
  const parts = uri.match(POST_URI);
  if (!parts) {
    res.status(400).json({ error: "InvalidRequest", message: "uri must be an at:// URI of a Bluesky post" });
    return;
  }
  const m = liveMatch(uri);
  if (!m) {
    res.status(404).json(NOT_FOUND);
    return;
  }

  let post: PostInfo;
  try {
    post = await postInfo(uri);
  } catch (e) {
    res.status(502).json({ error: "BlueskyUnavailable", message: "Could not reach Bluesky to check the post." });
    return;
  }
  if (!post.visible) {
    res.status(404).json(NOT_FOUND);
    return;
  }

  // Entries from before the matcher logged all three candidates have only the chosen one.
  const candidates: Candidate[] = m.candidates?.length
    ? m.candidates
    : [{ rank: m.rank, note_id: m.note_id, tweet_id: m.tweet_id, cosine: m.cosine, tfidf: m.tfidf,
         applicable: true, reason: m.reason, error: null }];

  let notes: DemoNotes | null = null;
  let notesError: string | null = null;
  if (m.library) {
    try {
      notes = await fetchDemoNotes(m.library, candidates.map((c) => c.note_id));
      if (!notes) notesError = "library snapshot no longer on the server";
    } catch {
      notesError = "matcher unavailable";
    }
  } else {
    notesError = "library not recorded for this match";
  }

  const tid = tidMillis(parts[2]);
  res.json({
    uri,
    did: parts[1],
    rkey: parts[2],
    handle: post.handle ?? null,
    posted_at: post.posted_at ?? (tid ? new Date(tid).toISOString() : null),
    labeled_at: m.ts,
    reply_uri: m.reply_uris?.[0] ?? null,
    library: notes?.library ?? (m.library ? { name: m.library } : null),
    thresholds: notes?.thresholds ?? null,
    scores: { tfidf_max: m.tfidf_max ?? null, cosine_max: m.cosine_max ?? null },
    chosen_note_id: m.note_id,
    candidates: candidates.map((c) => ({
      rank: c.rank,
      note_id: c.note_id,
      tweet_id: c.tweet_id,
      cosine: c.cosine,
      tfidf: c.tfidf,
      applicable: c.applicable,
      reason: c.reason,
      error: c.error ? "the model call failed" : null,
      note: (notes?.notes[c.note_id] as DemoNote | undefined) ?? null,
    })),
    notes_error: notesError,
  });
});

let summaryCache: { at: number; body: unknown } | null = null;

demoRouter.get("/summary", async (_req, res) => {
  if (summaryCache && Date.now() - summaryCache.at < SUMMARY_CACHE_MS) {
    res.json(summaryCache.body);
    return;
  }
  const live = liveMatches();

  // Post → label, with the post time taken from its record key.
  const labelDelays = live.flatMap((m) => {
    const t = tidMillis(m.uri.split("/").pop() ?? "");
    const d = t ? Date.parse(m.ts) - t : NaN;
    return Number.isFinite(d) && d >= 0 && d < 7 * 86400_000 ? [d] : [];
  });

  // X post → note first rated Helpful, for the notes CrossNote applied.
  const byLibrary = new Map<string, Set<string>>();
  for (const m of live) {
    if (!m.library) continue;
    if (!byLibrary.has(m.library)) byLibrary.set(m.library, new Set());
    byLibrary.get(m.library)!.add(m.note_id);
  }
  const xDelays: number[] = [];
  for (const [library, ids] of byLibrary) {
    try {
      const notes = await fetchDemoNotes(library, [...ids].slice(0, 200));
      for (const n of Object.values(notes?.notes ?? {})) {
        const d = Date.parse(n.note_helpful_at ?? "") - Date.parse(n.x_post_created_at ?? "");
        if (Number.isFinite(d) && d >= 0) xDelays.push(d);
      }
    } catch {
      // matcher down: leave the X numbers out
    }
  }

  let library: { name: string | null; notes: number } | null = null;
  try {
    const h = await fetchMatcherHealth();
    library = { name: h.library, notes: h.notes };
  } catch {}

  const body = {
    labeled_posts: live.length,
    median_label_delay_ms: median(labelDelays),
    median_x_delay_ms: median(xDelays),
    x_delay_notes: xDelays.length,
    library,
    generated_at: new Date().toISOString(),
  };
  summaryCache = { at: Date.now(), body };
  res.json(body);
});

demoRouter.post("/feedback", express.json({ limit: "4kb" }), (req, res) => {
  const origin = req.get("origin")?.replace(/\/+$/, "");
  if (!origin || !FEEDBACK_ORIGINS.has(origin)) {
    res.status(403).json({ error: "Forbidden", message: "Feedback is only accepted from the demo page." });
    return;
  }
  const { uri, answer, comment } = (req.body ?? {}) as Record<string, unknown>;
  if (typeof uri !== "string" || !POST_URI.test(uri) || (answer !== "yes" && answer !== "no" && answer !== "unsure")) {
    res.status(400).json({ error: "InvalidRequest", message: "uri and answer (yes, no or unsure) are required" });
    return;
  }
  if (comment != null && typeof comment !== "string") {
    res.status(400).json({ error: "InvalidRequest", message: "comment must be text" });
    return;
  }
  const m = liveMatch(uri);
  if (!m) {
    res.status(404).json(NOT_FOUND);
    return;
  }
  if (!feedbackAllowed(clientIp(req))) {
    res.status(429).json({ error: "RateLimited", message: "Too many answers. Please try again later." });
    return;
  }
  const text = typeof comment === "string" ? comment.trim().slice(0, COMMENT_MAX_CHARS) : "";
  db.logFeedback(uri, m.note_id, answer, text || null);
  res.json({ ok: true });
});

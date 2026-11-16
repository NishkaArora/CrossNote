// Client for the Python matcher (pipeline/crossnote_server.py) on the unix
// socket started by start.sh.
import { Agent, fetch } from "undici";

const SOCKET_PATH = process.env.PIPELINE_SOCKET ?? "/tmp/pipeline.sock";

const socketAgent = new Agent({
  connect: { socketPath: SOCKET_PATH, timeout: 5_000 },
});

// Max concurrent requests to Python and max posts waiting in the queue.
// Posts arriving when the queue is full are dropped (counted in `throttled`).
const MAX_CONCURRENT = 4;
const MAX_QUEUE      = 400;
const REQUEST_TIMEOUT_MS = 120_000;  // embedding + 3 Stage 3 calls with retries

export interface Stage3Judgment {
  rank: number;
  note_id: string;
  tweet_id: string;
  cosine: number;
  tfidf: number;
  applicable: boolean;
  reason: string | null;
  error: string | null;
}

export interface MatchedNote extends Stage3Judgment {
  note_summary: string;
  reply_note_text: string;  // summary with HTML entities decoded
  x_post_text: string;
}

export interface MatchResponse {
  stage: "not_ready" | "length" | "tfidf" | "embedding" | "stage3" | "error";
  library: string | null;
  tfidf_max?: number;
  cosine_max?: number;
  candidates?: Stage3Judgment[];
  match: MatchedNote | null;
  error?: string;
}

type Task = { text: string; uri: string; resolve: (r: MatchResponse) => void; reject: (e: unknown) => void };
const queue: Task[] = [];
let inFlight = 0;
export let throttled = 0;

async function callMatcher(text: string, uri: string): Promise<MatchResponse> {
  const response = await fetch("http://localhost/match", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ text, uri }),
    dispatcher: socketAgent,
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  if (!response.ok) throw new Error(`matcher → HTTP ${response.status}`);
  return (await response.json()) as MatchResponse;
}

function drain(): void {
  while (inFlight < MAX_CONCURRENT && queue.length > 0) {
    const task = queue.shift()!;
    inFlight++;
    callMatcher(task.text, task.uri)
      .then(task.resolve, task.reject)
      .finally(() => { inFlight--; drain(); });
  }
}

// ── Demo page lookups (src/demo-api.ts) ──────────────────────────────────────

export interface DemoNote {
  note_id: string;
  tweet_id: string;
  text: string;
  note_created_at: string | null;
  note_helpful_at: string | null;
  x_post_created_at: string | null;
}

export interface DemoNotes {
  library: { name: string; export_date: string | null; built_at: string | null; notes: number };
  thresholds: { min_words_exclusive: number | null; tfidf: number | null; embedding: number | null; top_k: number | null };
  notes: Record<string, DemoNote>;
}

/** Note text and timestamps from a library snapshot. Null if the snapshot is gone. */
export async function fetchDemoNotes(library: string, noteIds: string[]): Promise<DemoNotes | null> {
  const response = await fetch("http://localhost/demo/notes", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ library, note_ids: noteIds }),
    dispatcher: socketAgent,
    signal: AbortSignal.timeout(10_000),
  });
  if (response.status === 404) return null;
  if (!response.ok) throw new Error(`matcher /demo/notes → HTTP ${response.status}`);
  return (await response.json()) as DemoNotes;
}

/** The snapshot the matcher is using now. */
export async function fetchMatcherHealth(): Promise<{ ready: boolean; library: string | null; notes: number }> {
  const response = await fetch("http://localhost/health", { dispatcher: socketAgent, signal: AbortSignal.timeout(5_000) });
  if (!response.ok) throw new Error(`matcher /health → HTTP ${response.status}`);
  return (await response.json()) as { ready: boolean; library: string | null; notes: number };
}

export interface DemoRerun {
  stage: "not_ready" | "length" | "tfidf" | "embedding" | "stage3";
  library: string | null;
  tfidf_max?: number;
  cosine_max?: number;
  candidates?: (Stage3Judgment & { note_text: string | null })[];
  match: string | null;  // note ID
  thresholds: DemoNotes["thresholds"];
}

/** The pipeline on edited text, for the demo page: nothing is counted, logged or labelled. */
export async function rerunDemo(text: string, library: string | null): Promise<DemoRerun> {
  const response = await fetch("http://localhost/demo/rerun", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ text, library }),
    dispatcher: socketAgent,
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  if (!response.ok) throw new Error(`matcher /demo/rerun → HTTP ${response.status}`);
  return (await response.json()) as DemoRerun;
}

/** Runs the post through Stages 1-3. Resolves null if the queue is full. */
export function matchPost(text: string, uri: string): Promise<MatchResponse | null> {
  if (queue.length >= MAX_QUEUE) {
    throttled++;
    return Promise.resolve(null);
  }
  return new Promise((resolve, reject) => {
    queue.push({ text, uri, resolve, reject });
    drain();
  });
}

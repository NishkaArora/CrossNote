// Client for the Python matcher (pipeline/crossnote_server.py) on the unix
// socket started by start.sh. Replaces detect.ts for the follower stream.
import { Agent, fetch } from "undici";

const SOCKET_PATH = "/tmp/pipeline.sock";

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

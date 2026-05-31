import { Agent, fetch } from "undici";
import { existsSync } from "fs";

const SOCKET_PATH = "/tmp/pipeline.sock";

const socketAgent = new Agent({
  connect: { socketPath: SOCKET_PATH, timeout: 5_000 },
});

// Max concurrent requests to Python and max posts waiting in queue.
// Posts that arrive when the queue is full are dropped.
const MAX_CONCURRENT = 4;
const MAX_QUEUE      = 400;

// Pre-filter thresholds — cheap Node-side checks before touching Python.
const MIN_WORDS   = 8;    // too short to contain a checkable claim
const MIN_ASCII_RATIO = 0.5; // < 50% ASCII letters → likely non-English

type Task = { text: string; resolve: (r: DetectionResult | null) => void; reject: (e: unknown) => void };
const queue: Task[] = [];
let inFlight = 0;
export let throttled   = 0;
export let preFiltered = 0;

let socketReady = false;

export interface DetectionResult {
  label: "misinformation";
  note: string;
}

function passesPreFilter(text: string): boolean {
  const words = text.trim().split(/\s+/);
  if (words.length < MIN_WORDS) return false;

  // Count ASCII letters vs total letters to detect non-English scripts.
  let ascii = 0, total = 0;
  for (const ch of text) {
    if (/\p{L}/u.test(ch)) {
      total++;
      if (/[a-zA-Z]/.test(ch)) ascii++;
    }
  }
  if (total > 0 && ascii / total < MIN_ASCII_RATIO) return false;

  return true;
}

async function _callPipeline(text: string): Promise<DetectionResult | null> {
  const response = await fetch("http://localhost/analyze", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ text }),
    dispatcher: socketAgent,
  });

  if (!response.ok) return null;

  const result = await response.json() as { label: string | null; note: string | null };
  if (!result.label || !result.note) return null;

  return { label: "misinformation", note: result.note };
}

function pump() {
  while (inFlight < MAX_CONCURRENT && queue.length > 0) {
    const task = queue.shift()!;
    inFlight++;
    _callPipeline(task.text)
      .then(task.resolve, task.reject)
      .finally(() => { inFlight--; pump(); });
  }
}

export async function detectLabel(text: string): Promise<DetectionResult | null> {
  if (!passesPreFilter(text)) {
    preFiltered++;
    return null;
  }

  if (!socketReady) {
    if (!existsSync(SOCKET_PATH)) return null;
    socketReady = true;
  }

  if (queue.length >= MAX_QUEUE) {
    throttled++;
    return null;
  }

  try {
    return await new Promise<DetectionResult | null>((resolve, reject) => {
      queue.push({ text, resolve, reject });
      pump();
    });
  } catch {
    socketReady = false;
    return null;
  }
}

import { Agent, fetch } from "undici";
import { existsSync } from "fs";

const SOCKET_PATH = "/tmp/pipeline.sock";

const socketAgent = new Agent({
  connect: { socketPath: SOCKET_PATH, timeout: 5_000 },
});

// Max concurrent requests to Python and max posts waiting in queue.
// Posts that arrive when the queue is full are dropped.
const MAX_CONCURRENT = 5;
const MAX_QUEUE      = 200;

type Task = { text: string; resolve: (r: DetectionResult | null) => void; reject: (e: unknown) => void };
const queue: Task[] = [];
let inFlight = 0;
export let throttled = 0;

let socketReady = false;

export interface DetectionResult {
  label: "misinformation";
  note: string;
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
  const cleaned = text.replace(/\[crossnote test\]/gi, "").trim();
  if (!cleaned) return null;

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
      queue.push({ text: cleaned, resolve, reject });
      pump();
    });
  } catch {
    socketReady = false;
    return null;
  }
}

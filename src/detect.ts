import { Agent, fetch } from "undici";
import { existsSync } from "fs";

const SOCKET_PATH = "/tmp/pipeline.sock";

const socketAgent = new Agent({
  connect: { socketPath: SOCKET_PATH, timeout: 5_000 },
});

// Max concurrent requests to the Python pipeline.
// Posts that arrive while Python is busy are dropped (not queued).
const MAX_IN_FLIGHT = 5;
let inFlight = 0;
export let throttled = 0;

// Cached once the socket file is first seen — avoids repeated fs calls.
let socketReady = false;

export interface DetectionResult {
  label: "misinformation";
  note: string;
}

export async function detectLabel(text: string): Promise<DetectionResult | null> {
  const cleaned = text.replace(/\[crossnote test\]/gi, "").trim();
  if (!cleaned) return null;

  if (!socketReady) {
    if (!existsSync(SOCKET_PATH)) return null;
    socketReady = true;
  }

  if (inFlight >= MAX_IN_FLIGHT) { throttled++; return null; }

  inFlight++;
  try {
    const response = await fetch("http://localhost/analyze", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text: cleaned }),
      dispatcher: socketAgent,
    });

    if (!response.ok) return null;

    const result = await response.json() as { label: string | null; note: string | null };
    if (!result.label || !result.note) return null;

    return { label: "misinformation", note: result.note };
  } finally {
    inFlight--;
  }
}

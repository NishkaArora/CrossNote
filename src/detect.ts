import { Agent, fetch } from "undici";

// Persistent agent for Unix socket communication with the Python pipeline
const socketAgent = new Agent({
  connect: { socketPath: "/tmp/pipeline.sock", timeout: 30_000 },
});

export interface DetectionResult {
  label: "misinformation";
  note: string;  // CN summary — used as the reply comment
}

export async function detectLabel(text: string): Promise<DetectionResult | null> {
  const cleaned = text.replace(/\[crossnote test\]/gi, "").trim();
  if (!cleaned) return null;

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
}

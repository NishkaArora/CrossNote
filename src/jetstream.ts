import { Jetstream } from "@skyware/jetstream";
import { LabelerServer } from "./labeler-server.js";
import { detectLabel, throttled } from "./detect.js";
import { postComment } from "./comment.js";
import * as db from "./db.js";

const BSKY_CHAR_LIMIT = 300;
const COMMENT_PREFIX  = "Similar claims were fact-checked on X: ";

function formatComment(note: string): string {
  const available = BSKY_CHAR_LIMIT - COMMENT_PREFIX.length;
  const truncated  = note.length > available ? note.slice(0, available - 1) + "…" : note;
  return COMMENT_PREFIX + truncated;
}

export function startJetstream(labeler: LabelerServer): void {
  const jetstream = new Jetstream({
    wantedCollections: ["app.bsky.feed.post"],
    cursor: db.getCursor(),
  });

  let processed = 0;
  let labeled = 0;
  let errors = 0;

  setInterval(() => {
    console.log(`Stats: ${processed} processed, ${labeled} labeled, ${errors} errors, ${throttled} throttled`);
  }, 60_000);

  jetstream.on("commit", async (event) => {
    if (event.commit.operation === "delete") return;

    processed++;

    if (processed % 100 === 0) {
      db.setCursor(event.time_us);
    }

    const record = event.commit.record as { text?: string };
    if (!record.text) return;

    try {
      const result = await detectLabel(record.text);
      if (!result) return;

      labeled++;
      const uri = `at://${event.did}/${event.commit.collection}/${event.commit.rkey}`;

      console.log(`[LABEL] ${result.label} → ${uri}`);
      labeler.emitLabel(uri, result.label, event.commit.cid);
      db.logLabel(uri, event.commit.cid, result.label, record.text, result.note);

      if (event.commit.cid) {
        postComment(uri, event.commit.cid, formatComment(result.note)).catch((e) =>
          console.error("Failed to post comment:", e)
        );
      }
    } catch (e) {
      errors++;
      if (errors % 50 === 1) {
        console.error("Detection error:", e instanceof Error ? e.message : e);
      }
    }
  });

  jetstream.on("error", (error) => {
    console.error("Jetstream error:", error);
  });

  jetstream.start();
  console.log("Jetstream connected, watching all posts...");
}

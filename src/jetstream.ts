import { Jetstream } from "@skyware/jetstream";
import { LabelerServer } from "./labeler-server.js";
import { detectLabel } from "./detect.js";
import { postComment } from "./comment.js";
import * as db from "./db.js";

export function startJetstream(labeler: LabelerServer): void {
  const jetstream = new Jetstream({
    wantedCollections: ["app.bsky.feed.post"],
    cursor: db.getCursor(),
  });

  let processed = 0;
  let labeled = 0;

  setInterval(() => {
    console.log(`Stats: ${processed} posts processed, ${labeled} labeled`);
  }, 60_000);

  jetstream.on("commit", async (event) => {
    if (event.commit.operation === "delete") return;

    processed++;

    // Persist cursor every 100 events so restarts don't reprocess too much
    if (processed % 100 === 0) {
      db.setCursor(event.time_us);
    }

    const record = event.commit.record as { text?: string };
    if (!record.text) return;

    const label = detectLabel(record.text);
    if (!label) return;

    labeled++;
    const uri = `at://${event.did}/${event.commit.collection}/${event.commit.rkey}`;

    const comment = "dummy comment";

    console.log(`[LABEL] ${label} → ${uri}`);
    labeler.emitLabel(uri, label, event.commit.cid);
    db.logLabel(uri, event.commit.cid, label, record.text, comment);

    if (event.commit.cid) {
      postComment(uri, event.commit.cid, comment).catch((e) =>
        console.error("Failed to post comment:", e)
      );
    }
  });

  jetstream.on("error", (error) => {
    console.error("Jetstream error:", error);
  });

  jetstream.start();
  console.log("Jetstream connected, watching all posts...");
}

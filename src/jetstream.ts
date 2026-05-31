import { Jetstream } from "@skyware/jetstream";
import { appendFileSync } from "fs";
import { LabelerServer } from "./labeler-server.js";
import { detectLabel, throttled } from "./detect.js";
import { postComment } from "./comment.js";
import * as db from "./db.js";

const BSKY_CHAR_LIMIT = 300;
const COMMENT_PREFIX  = "Similar claims were fact-checked on X: ";
const RATE_CSV_PATH   = "/data/jetstream_rate.csv";

function formatComment(note: string): string {
  const available = BSKY_CHAR_LIMIT - COMMENT_PREFIX.length;
  const truncated  = note.length > available ? note.slice(0, available - 1) + "…" : note;
  return COMMENT_PREFIX + truncated;
}

export interface JetstreamStats {
  processed: number;
  labeled: number;
  errors: number;
  throttled: number;
  postsPerSec: number;
}

export let currentStats: JetstreamStats = {
  processed: 0, labeled: 0, errors: 0, throttled: 0, postsPerSec: 0,
};

export function startJetstream(labeler: LabelerServer): void {
  const jetstream = new Jetstream({
    wantedCollections: ["app.bsky.feed.post"],
    cursor: db.getCursor(),
  });

  let processed = 0;
  let labeled = 0;
  let errors = 0;
  let intervalPosts = 0;
  let intervalStart = Date.now();

  // Write CSV header if file doesn't exist yet.
  try {
    appendFileSync(RATE_CSV_PATH, "timestamp,posts_per_sec,processed,labeled,errors,throttled\n", { flag: "ax" });
  } catch {}

  setInterval(() => {
    const elapsed = (Date.now() - intervalStart) / 1000;
    const postsPerSec = parseFloat((intervalPosts / elapsed).toFixed(2));
    const ts = new Date().toISOString();

    console.log(`Stats: ${processed} processed, ${labeled} labeled, ${errors} errors, ${throttled} throttled, ${postsPerSec} posts/sec`);

    try {
      appendFileSync(RATE_CSV_PATH, `${ts},${postsPerSec},${processed},${labeled},${errors},${throttled}\n`);
    } catch (e) {
      console.error("Failed to write rate CSV:", e instanceof Error ? e.message : e);
    }

    currentStats = { processed, labeled, errors, throttled, postsPerSec };
    intervalPosts = 0;
    intervalStart = Date.now();
  }, 60_000);

  jetstream.on("commit", async (event) => {
    if (event.commit.operation === "delete") return;

    processed++;
    intervalPosts++;

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

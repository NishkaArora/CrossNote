/**
 * Shows the most recent labeled posts from the database.
 * Usage: npx tsx scripts/check-labels.ts [limit]
 *
 * Run on Fly.io: fly ssh console -C "npx tsx scripts/check-labels.ts 20"
 */

import Database from "better-sqlite3";

const limit = parseInt(process.argv[2] ?? "10", 10);
const DB_PATH = process.env.DATABASE_URL?.replace("file:", "") ?? "labeler.db";
const db = new Database(DB_PATH);

const rows = db
  .prepare(
    "SELECT id, val, uri, text, createdAt FROM labels ORDER BY id DESC LIMIT ?"
  )
  .all(limit) as { id: number; val: string; uri: string; text: string; createdAt: string }[];

if (rows.length === 0) {
  console.log("No labels in database yet.");
} else {
  console.log(`Last ${rows.length} label(s):\n`);
  for (const row of rows) {
    console.log(`[${row.id}] ${row.createdAt}`);
    console.log(`  Label : ${row.val}`);
    console.log(`  URI   : ${row.uri}`);
    console.log(`  Text  : ${row.text.slice(0, 120)}`);
    console.log();
  }
}

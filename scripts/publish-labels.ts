/**
 * One-time setup script: declares the "misinformation" label on the CrossNote
 * labeler account. Run this once locally after the account is created:
 *
 *   npx tsx scripts/publish-labels.ts
 */

import "dotenv/config";
import { AtpAgent } from "@atproto/api";

const { BSKY_HANDLE, BSKY_PASSWORD } = process.env;

if (!BSKY_HANDLE || !BSKY_PASSWORD) {
  console.error("BSKY_HANDLE and BSKY_PASSWORD must be set in .env");
  process.exit(1);
}

const agent = new AtpAgent({ service: "https://bsky.social" });
await agent.login({ identifier: BSKY_HANDLE, password: BSKY_PASSWORD });

await agent.com.atproto.repo.putRecord({
  repo: agent.session!.did,
  collection: "app.bsky.labeler.service",
  rkey: "self",
  record: {
    createdAt: new Date().toISOString(),
    policies: {
      labelValues: ["misinformation"],
      labelValueDefinitions: [
        {
          identifier: "misinformation",
          severity: "inform",
          blurs: "none",
          defaultSetting: "warn",
          locales: [
            {
              lang: "en",
              name: "Misinformation",
              description:
                "This post may contain misinformation. Check the comments for context.",
            },
          ],
        },
      ],
    },
  },
});

console.log("Label definitions published successfully.");

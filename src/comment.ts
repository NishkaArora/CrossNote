import { AtpAgent } from "@atproto/api";

const agent = new AtpAgent({ service: "https://bsky.social" });
let loggedIn = false;

async function ensureLoggedIn(): Promise<void> {
  if (loggedIn) return;
  await agent.login({
    identifier: process.env.BSKY_HANDLE!,
    password: process.env.BSKY_PASSWORD!,
  });
  loggedIn = true;
}

// Posts a reply to the given post URI/CID with the provided comment text.
// comment will eventually be a description string from the detection pipeline.
export async function postComment(
  uri: string,
  cid: string,
  comment: string
): Promise<void> {
  await ensureLoggedIn();
  await agent.post({
    text: comment,
    reply: {
      root: { uri, cid },
      parent: { uri, cid },
    },
  });
}

// The reply posted under a matched post: the full Community Note, threaded
// when it exceeds 300 characters. Port of build_posts in
// crossnote-app/scripts/publish_cn_sample.py (same tokenising, link facets,
// chunking and " (i/n)" suffixes), with this header and a closing link to the
// original X post.
import { AtpAgent } from "@atproto/api";

export const REPLY_HEADER = "Community Note from X, rated Helpful:\n\n";
const MAX_GRAPHEMES = 300;  // Bluesky post length limit
const LINK_DISPLAY  = 32;   // max chars of a shortened link's visible text

export interface Facet {
  index: { byteStart: number; byteEnd: number };
  features: { $type: "app.bsky.richtext.facet#link"; uri: string }[];
}
export interface PostPart { text: string; facets: Facet[] }

type Token = [text: string, url: string | null];

const URL_RE   = /^https?:\/\/\S+$/;
const TOKEN_RE = /https?:\/\/\S+|\S+|\s+/g;
const encoder  = new TextEncoder();

// Code points, as Python's len(); >= grapheme count, so conservative.
const cpLen = (s: string) => [...s].length;
const isSpace = (s: string) => /^\s+$/.test(s);

function shortenUrl(url: string): string {
  const display = url.replace(/^https?:\/\/(www\.)?/, "");
  return cpLen(display) <= LINK_DISPLAY ? display : [...display].slice(0, LINK_DISPLAY - 1).join("") + "…";
}

function tokenize(text: string): Token[] {
  const tokens: Token[] = [];
  for (const m of text.matchAll(TOKEN_RE)) {
    const tok = m[0];
    if (URL_RE.test(tok)) {
      const url = tok.replace(/[.,;:!?)"']+$/, "");
      tokens.push([shortenUrl(url), url]);
      if (url.length < tok.length) tokens.push([tok.slice(url.length), null]);
    } else {
      tokens.push([tok, null]);
    }
  }
  return tokens;
}

function splitLong(tokens: Token[], limit: number): Token[] {
  const out: Token[] = [];
  for (const [text, url] of tokens) {
    if (cpLen(text) <= limit || url) {
      out.push([text, url]);
    } else {
      const cps = [...text];
      for (let i = 0; i < cps.length; i += limit) out.push([cps.slice(i, i + limit).join(""), null]);
    }
  }
  return out;
}

function chunkTokens(tokens: Token[], limit: number): Token[][] {
  const chunks: Token[][] = [];
  let cur: Token[] = [];
  let curLen = 0;
  for (const [text, url] of tokens) {
    if (isSpace(text) && cur.length === 0) continue;  // no leading whitespace in a chunk
    if (curLen + cpLen(text) > limit && !isSpace(text)) {
      chunks.push(cur);
      cur = [];
      curLen = 0;
    }
    cur.push([text, url]);
    curLen += cpLen(text);
  }
  if (cur.length) chunks.push(cur);
  for (const c of chunks) while (c.length && isSpace(c[c.length - 1][0])) c.pop();
  return chunks.filter((c) => c.length > 0);
}

function render(tokens: Token[], suffix = ""): PostPart {
  let text = "";
  let pos = 0;
  const facets: Facet[] = [];
  for (const [tok, url] of tokens) {
    const b = encoder.encode(tok).length;
    if (url) {
      facets.push({
        index: { byteStart: pos, byteEnd: pos + b },
        features: [{ $type: "app.bsky.richtext.facet#link", uri: url }],
      });
    }
    text += tok;
    pos += b;
  }
  return { text: text + suffix, facets };
}

export function xPostUrl(tweetId: string): string {
  return `https://x.com/i/status/${tweetId}`;
}

/** Header + note + link to the original X post, as one or more posts ≤ 300 chars. */
export function buildPosts(noteText: string, tweetId: string): PostPart[] {
  const note = noteText.trim().replace(/[ \t]+/g, " ");
  const body = `${REPLY_HEADER}${note}\n\n${xPostUrl(tweetId)}`;
  const tokens = tokenize(body);

  if (tokens.reduce((n, [t]) => n + cpLen(t), 0) <= MAX_GRAPHEMES) return [render(tokens)];

  const limit = MAX_GRAPHEMES - " (99/99)".length;
  const chunks = chunkTokens(splitLong(tokens, limit), limit);
  return chunks.map((c, i) => render(c, ` (${i + 1}/${chunks.length})`));
}

// ── Posting ──────────────────────────────────────────────────────────────────

const agent = new AtpAgent({ service: "https://bsky.social" });
let loggedIn = false;

async function ensureLoggedIn(): Promise<void> {
  if (loggedIn) return;
  await agent.login({ identifier: process.env.BSKY_HANDLE!, password: process.env.BSKY_PASSWORD! });
  loggedIn = true;
}

/** Posts `parts` as a thread under the post at uri/cid. Returns the reply URIs. */
export async function postReplyThread(uri: string, cid: string, parts: PostPart[]): Promise<string[]> {
  await ensureLoggedIn();
  const root = { uri, cid };
  let parent = root;
  const uris: string[] = [];
  for (const part of parts) {
    const res = await agent.post({
      text: part.text,
      facets: part.facets.length ? part.facets : undefined,
      langs: ["en"],
      reply: { root, parent },
      createdAt: new Date().toISOString(),
    });
    uris.push(res.uri);
    parent = { uri: res.uri, cid: res.cid };
  }
  return uris;
}

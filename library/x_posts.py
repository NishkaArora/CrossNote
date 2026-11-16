"""Original X posts for library notes, via the X API v2 tweet lookup
(GET /2/tweets, up to 100 IDs per request).

tweet.fields, expansions and media.fields come from configs/pipeline.yaml,
so each record carries the post's `lang` and media types, which the library
filters need. Never scrapes. Needs your own X_BEARER_TOKEN; retrieved post
text must not be redistributed (X Developer Agreement and Policy).
"""
from __future__ import annotations

import time

import requests

API_URL = "https://api.twitter.com/2/tweets"
BATCH_SIZE = 100  # GET /2/tweets accepts up to 100 IDs per request
MAX_RETRIES = 5


def fetch_batch_with_media(ids, token, tweet_fields, expansions, media_fields):
    """-> (tweets, media, errors, ok). ok=False means the whole batch failed
    (non-200 or retries exhausted), so its IDs are not known to be gone."""
    headers = {"Authorization": f"Bearer {token}"}
    params = {"ids": ",".join(ids), "tweet.fields": tweet_fields,
              "expansions": expansions, "media.fields": media_fields}

    for attempt in range(MAX_RETRIES):
        resp = requests.get(API_URL, headers=headers, params=params, timeout=30)

        if resp.status_code == 200:
            body = resp.json()
            return (body.get("data") or [], (body.get("includes") or {}).get("media") or [],
                    body.get("errors") or [], True)

        if resp.status_code == 429:
            reset = int(resp.headers.get("x-rate-limit-reset", time.time() + 60))
            wait = max(reset - time.time(), 1) + 5
            print(f"  Rate-limited -- sleeping {wait:.0f}s")
            time.sleep(wait)
            continue

        if resp.status_code in (500, 502, 503):
            backoff = 2**attempt
            print(f"  HTTP {resp.status_code} (transient) -- retrying in {backoff}s")
            time.sleep(backoff)
            continue

        print(f"  HTTP {resp.status_code}: {resp.text[:200]}")
        return [], [], [], False

    print(f"  Giving up on this batch after {MAX_RETRIES} attempts.")
    return [], [], [], False


def fetch_posts(tweet_ids, token, cfg, sleep_seconds=0.5):
    """tweetId -> record for every post the API returned, and the set of IDs
    whose batch failed outright (to retry on a later build).

    record = {"text", "lang", "created_at", "tweet_has_media" (0/1),
              "tweet_media_type" (comma-joined sorted types, or None)}
    """
    lib = cfg["library"]
    found, failed = {}, set()
    for i in range(0, len(tweet_ids), BATCH_SIZE):
        batch = tweet_ids[i:i + BATCH_SIZE]
        tweets, media, errors, ok = fetch_batch_with_media(
            batch, token, lib["x_tweet_fields"], lib["x_expansions"], lib["x_media_fields"])
        if not ok:
            failed.update(batch)
            continue
        mk_to_type = {m["media_key"]: m.get("type", "") for m in media}
        for t in tweets:
            keys = (t.get("attachments") or {}).get("media_keys") or []
            types = sorted({mk_to_type[k] for k in keys if mk_to_type.get(k)})
            found[t["id"]] = {
                "text": t.get("text", "") or "",
                "lang": t.get("lang"),
                "created_at": t.get("created_at"),
                "tweet_has_media": 1 if keys else 0,
                "tweet_media_type": ",".join(types) if types else None,
            }
        print(f"  X batch {i // BATCH_SIZE + 1}/{(len(tweet_ids) - 1) // BATCH_SIZE + 1}: "
              f"{len(tweets)} retrieved, {len(errors)} unavailable")
        time.sleep(sleep_seconds)
    return found, failed

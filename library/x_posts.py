"""Original X posts for library notes, via the X API v2 tweet lookup.

Uses rehydrate_x_posts.py (copied unchanged from the released artefact) for
the endpoint, batch size and retry policy. Its fetch_batch hard-codes the
request params and drops the response's `includes`, so it can't return the
`lang` and media types the library needs. fetch_batch_with_media below is
fetch_batch with three changes:
  * tweet.fields from configs/pipeline.yaml, which adds lang and attachments;
  * expansions=attachments.media_keys and media.fields=media_key,type, the
    same media request as the End-to-End Precision Evaluation notebook's
    fetch_media_batch;
  * it also returns includes.media.
Never scrapes. Needs X_BEARER_TOKEN.
"""
from __future__ import annotations

import time

import requests

from library import rehydrate_x_posts as rx


def fetch_batch_with_media(ids, token, tweet_fields, expansions, media_fields):
    """-> (tweets, media, errors, ok). ok=False means the whole batch failed
    (non-200 or retries exhausted), so its IDs are not known to be gone."""
    headers = {"Authorization": f"Bearer {token}"}
    params = {"ids": ",".join(ids), "tweet.fields": tweet_fields,
              "expansions": expansions, "media.fields": media_fields}

    for attempt in range(rx.MAX_RETRIES):
        resp = requests.get(rx.API_URL, headers=headers, params=params, timeout=30)

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

    print(f"  Giving up on this batch after {rx.MAX_RETRIES} attempts.")
    return [], [], [], False


def fetch_posts(tweet_ids, token, cfg, sleep_seconds=0.5):
    """tweetId -> record for every post the API returned, and the set of IDs
    whose batch failed outright (to retry on a later build).

    record = {"text", "lang", "created_at", "tweet_has_media" (0/1),
              "tweet_media_type" (comma-joined sorted types, or None)}
    """
    lib = cfg["library"]
    found, failed = {}, set()
    for i in range(0, len(tweet_ids), rx.BATCH_SIZE):
        batch = tweet_ids[i:i + rx.BATCH_SIZE]
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
        print(f"  X batch {i // rx.BATCH_SIZE + 1}/{(len(tweet_ids) - 1) // rx.BATCH_SIZE + 1}: "
              f"{len(tweets)} retrieved, {len(errors)} unavailable")
        time.sleep(sleep_seconds)
    return found, failed

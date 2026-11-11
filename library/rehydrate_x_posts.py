#!/usr/bin/env python3
"""Rehydrate the original X (Twitter) posts referenced by this release.

Reads the released `x_post_id` values (from data/labels/*.csv and any other
released file that carries one), fetches each currently-available post via
the X API v2 `GET /2/tweets` endpoint, and saves the results to a local,
gitignored directory. Requires your own X API bearer token.

Rehydration retrieves records that remain available at the time this script
is run. It does NOT reconstruct the exact historical corpus used in the
paper -- posts may have since been edited, deleted, made private, or
otherwise become unavailable. Rehydrated content must not be recommitted or
redistributed publicly; you are responsible for complying with the X
Developer Agreement and Policy for any content you retrieve.

Setup:
    export X_BEARER_TOKEN="your-own-token-here"   # never hardcode this

Usage:
    python rehydrate_x_posts.py --artifact-root /path/to/artifact --out-dir downloads/

Request shape (endpoint, headers, rate-limit handling) matches a previously
verified working call against this same API from this project's own private
tweet-rehydration notebook -- not independently re-verified live here, since
that requires a bearer token this environment doesn't have.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import requests

API_URL = "https://api.twitter.com/2/tweets"
BATCH_SIZE = 100  # GET /2/tweets accepts up to 100 IDs per request
TWEET_FIELDS = "id,text,author_id,created_at,public_metrics"
MAX_RETRIES = 5


def collect_released_x_post_ids(artifact_root: str) -> set[str]:
    data_dir = os.path.join(artifact_root, "data")
    ids: set[str] = set()
    for path in glob.glob(os.path.join(data_dir, "**", "*.csv"), recursive=True):
        try:
            header = pd.read_csv(path, nrows=0).columns
        except Exception:
            continue
        if "x_post_id" not in header:
            continue
        col = pd.read_csv(path, usecols=["x_post_id"], dtype={"x_post_id": str})
        ids.update(col["x_post_id"].dropna().tolist())
    return ids


def fetch_batch(ids: list[str], token: str) -> tuple[list[dict], list[dict]]:
    """Returns (tweets, errors). Retries on rate limits (429) and transient
    5xx errors; on any other non-200 status, logs and returns empty (the
    caller records the whole batch as unavailable)."""
    headers = {"Authorization": f"Bearer {token}"}
    params = {"ids": ",".join(ids), "tweet.fields": TWEET_FIELDS}

    for attempt in range(MAX_RETRIES):
        resp = requests.get(API_URL, headers=headers, params=params, timeout=30)

        if resp.status_code == 200:
            body = resp.json()
            # the API can return an explicit null for "data" (e.g. all IDs invalid)
            return body.get("data") or [], body.get("errors") or []

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
        return [], []

    print(f"  Giving up on this batch after {MAX_RETRIES} attempts.")
    return [], []


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--artifact-root",
        default=os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..")),
    )
    parser.add_argument("--out-dir", default="downloads", help="Local output directory (should be gitignored)")
    parser.add_argument("--sleep-seconds", type=float, default=0.5, help="Delay between successful batches")
    args = parser.parse_args()

    token = os.environ.get("X_BEARER_TOKEN")
    if not token:
        print("ERROR: set the X_BEARER_TOKEN environment variable with your own X API bearer token.", file=sys.stderr)
        sys.exit(1)

    ids = sorted(collect_released_x_post_ids(args.artifact_root))
    print(f"Released X post IDs referenced by this artifact: {len(ids)}")
    if not ids:
        print("No x_post_id columns found under artifact-root/data -- nothing to rehydrate.", file=sys.stderr)
        sys.exit(1)

    os.makedirs(args.out_dir, exist_ok=True)
    posts_path = os.path.join(args.out_dir, "x_posts_rehydrated.jsonl")
    errors_path = os.path.join(args.out_dir, "x_posts_unavailable.csv")

    # Resumable: skip IDs already checkpointed from a prior (possibly
    # interrupted) run, so re-running this script after a failure only
    # fetches what's still missing.
    fetched_ids = set()
    if os.path.exists(posts_path):
        with open(posts_path) as f:
            for line in f:
                try:
                    fetched_ids.add(json.loads(line)["id"])
                except (json.JSONDecodeError, KeyError):
                    pass
        print(f"Resuming -- {len(fetched_ids)} posts already checkpointed in {posts_path}")

    remaining = [pid for pid in ids if pid not in fetched_ids]
    retrieved_at = datetime.now(timezone.utc).isoformat()

    n_retrieved = 0
    all_errors = []
    with open(posts_path, "a") as post_f:
        for i in range(0, len(remaining), BATCH_SIZE):
            batch = remaining[i : i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            n_batches = (len(remaining) - 1) // BATCH_SIZE + 1

            tweets, errors = fetch_batch(batch, token)
            for post in tweets:
                post["retrieved_at"] = retrieved_at
                post_f.write(json.dumps(post) + "\n")
            post_f.flush()
            n_retrieved += len(tweets)

            for err in errors:
                all_errors.append(
                    {
                        "x_post_id": err.get("resource_id"),
                        "reason": err.get("title", "unknown_error"),
                        "detail": err.get("detail", ""),
                    }
                )

            # any ID in this batch that came back neither as data nor as a
            # named error (e.g. the whole batch failed after retries) is
            # still logged as unavailable, not silently dropped.
            returned_or_errored = {t["id"] for t in tweets} | {e.get("resource_id") for e in errors}
            for pid in batch:
                if pid not in returned_or_errored:
                    all_errors.append({"x_post_id": pid, "reason": "no_response_after_retries", "detail": ""})

            print(f"Batch {batch_num}/{n_batches}: {len(tweets)} retrieved, {len(errors)} unavailable.")
            time.sleep(args.sleep_seconds)

    pd.DataFrame(all_errors).to_csv(errors_path, index=False)

    print(f"\nRetrieved {n_retrieved} new posts this run ({len(fetched_ids) + n_retrieved} total) -> {posts_path}")
    print(f"Logged {len(all_errors)} unavailable/errored IDs -> {errors_path}")
    print(
        "\nNOTE: these are the posts currently available via the X API -- deleted, "
        "suspended, or protected accounts' posts will not be retrievable and are "
        "logged above, not fabricated. Do not recommit or redistribute this content "
        f"publicly; '{args.out_dir}' should be listed in .gitignore. You are responsible "
        "for complying with the X Developer Agreement and Policy."
    )


if __name__ == "__main__":
    main()

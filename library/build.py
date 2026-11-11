#!/usr/bin/env python3
"""Build a dated note-library snapshot for the live CrossNote pipeline.

    data/library/<export-date>/
        notes.parquet          one row per kept note; row i <-> embeddings[i] <-> TF-IDF row i
        embeddings.npy         text-embedding-3-large, L2-normalised, float32
        tfidf_vectorizer.pkl   unigram TfidfVectorizer fitted on the note documents
        tfidf_matrix.pkl       the fitted TF-IDF matrix (scipy sparse)
        candidates.parquet     every note in the window, with the step that dropped it
        manifest.json          counts at each step, settings, prompt hashes

Steps:
  1. noteStatusHistory: CURRENTLY_RATED_HELPFUL now, and first rated Helpful
     (firstNonNMRStatus == CURRENTLY_RATED_HELPFUL) in the LIBRARY_WINDOW_DAYS
     days before the export date.
  2. Join note summaries from the notes shards (drop empty summaries).
  3. Fetch each note's original X post (X API v2). Drop notes whose post
     isn't returned.
  4. Keep posts with lang == "en".
  5. Drop author-dependent notes (gpt-6.1-sol, prompts/author_dependence.txt).
  6. Drop media-dependent notes (gpt-6.1-sol, prompts/media_dependence.txt),
     run only on notes that passed step 5.
  7. Embed "<X post text> <note summary>" with text-embedding-3-large.
  8. Fit the TF-IDF vectorizer on the same documents.

Until X_BEARER_TOKEN is available, --stub-x replaces step 3 with empty post
text and skips steps 4-6 (the filters need the post text). A stub build is written to <date>-STUB, is marked in
its manifest, and the live matcher refuses to load it.

LLM labels, X posts and embeddings are cached in data/library/_cache, so a
daily rebuild only pays for notes that are new to the window.

Usage (from the repo root):
    .venv/bin/python -m library.build --cn-dir ../../crossnote-app/backend/data
    .venv/bin/python -m library.build --cn-dir ... --sample 400            # random 400 of the window
    .venv/bin/python -m library.build --cn-dir ... --stub-x --sample 20    # test run, no X API
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
import pickle
import sys
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

from library.common import (REPO_ROOT, build_tfidf_index, embed_openai, l2_normalise,
                            load_config, load_prompt, note_document)

HELPFUL = "CURRENTLY_RATED_HELPFUL"
DEFAULT_OUT_ROOT = REPO_ROOT / "data" / "library"
NOTE_COLS = ["noteId", "tweetId", "summary", "classification", "createdAtMillis"]
STATUS_COLS = ["noteId", "currentStatus", "firstNonNMRStatus", "timestampMillisOfFirstNonNMRStatus"]


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def load_env_file(path: Path):
    """KEY=VALUE lines into os.environ (existing variables win)."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


# ── Community Notes export ───────────────────────────────────────────────────
# Read settings match download_community_notes.load_notes/load_status
# (sep="\t", noteId/tweetId as str, low_memory=False). Those loaders only read
# unzipped .tsv files; the export here is zipped, so this reads either.

def _tsv_sources(cn_dir: Path, prefix: str):
    """(name, opener) for each <prefix>-*.zip / <prefix>-*.tsv / <prefix>.zip."""
    out = []
    for p in sorted(glob.glob(str(cn_dir / f"{prefix}*"))):
        path = Path(p)
        if path.suffix == ".tsv" and path.stat().st_size > 0:
            out.append((path.name, lambda path=path: open(path, "rb")))
        elif path.suffix == ".zip":
            with zipfile.ZipFile(path) as z:
                members = [m for m in z.namelist() if m.endswith(".tsv")]
            for m in members:
                out.append((f"{path.name}:{m}",
                            lambda path=path, m=m: zipfile.ZipFile(path).open(m)))
    return out


def export_date_of(cn_dir: Path):
    """Date of the noteStatusHistory file inside the export."""
    for p in sorted(glob.glob(str(cn_dir / "noteStatusHistory*"))):
        path = Path(p)
        if path.suffix == ".zip":
            with zipfile.ZipFile(path) as z:
                info = next(i for i in z.infolist() if i.filename.endswith(".tsv"))
                return datetime(*info.date_time[:3]).date()
        if path.suffix == ".tsv" and path.stat().st_size > 0:
            return datetime.fromtimestamp(path.stat().st_mtime).date()
    raise FileNotFoundError(f"No noteStatusHistory file under {cn_dir}")


def load_window(cn_dir: Path, window_start_ms: int, window_end_ms: int, include_ids=frozenset()):
    """-> (window notes, hand-picked notes, total status rows). Hand-picked
    notes (include_ids) skip the window but must be Helpful now."""
    sources = _tsv_sources(cn_dir, "noteStatusHistory")
    if not sources:
        raise FileNotFoundError(f"No noteStatusHistory file under {cn_dir}")
    frames, picked, total = [], [], 0
    for name, opener in sources:
        log(f"reading {name}")
        with opener() as f:
            for chunk in pd.read_csv(f, sep="\t", dtype={"noteId": str}, usecols=STATUS_COLS,
                                     low_memory=False, chunksize=500_000):
                total += len(chunk)
                ts = chunk["timestampMillisOfFirstNonNMRStatus"]
                keep = ((chunk["currentStatus"] == HELPFUL)
                        & (chunk["firstNonNMRStatus"] == HELPFUL)
                        & (ts >= window_start_ms) & (ts < window_end_ms))
                frames.append(chunk[keep])
                if include_ids:
                    picked.append(chunk[chunk["noteId"].isin(include_ids)])

    def tidy(fs):
        out = pd.concat(fs, ignore_index=True).drop_duplicates("noteId") if fs else pd.DataFrame(columns=STATUS_COLS)
        return out.rename(columns={"timestampMillisOfFirstNonNMRStatus": "firstHelpfulMillis"})

    picked = tidy(picked)
    for nid in sorted(include_ids):
        row = picked[picked["noteId"] == nid]
        if row.empty:
            log(f"WARNING: hand-picked note {nid} is not in noteStatusHistory; skipped")
        elif row["currentStatus"].iloc[0] != HELPFUL:
            log(f"WARNING: hand-picked note {nid} is {row['currentStatus'].iloc[0]}, not Helpful; skipped")
    picked = picked[picked["currentStatus"] == HELPFUL]
    return tidy(frames)[["noteId", "firstHelpfulMillis"]], picked[["noteId", "firstHelpfulMillis"]], total


def load_notes(cn_dir: Path, note_ids: set):
    sources = _tsv_sources(cn_dir, "notes-")
    if not sources:
        raise FileNotFoundError(f"No notes-* shards under {cn_dir}")
    frames = []
    for name, opener in sources:
        log(f"reading {name}")
        with opener() as f:
            for chunk in pd.read_csv(f, sep="\t", dtype={"noteId": str, "tweetId": str},
                                     usecols=NOTE_COLS, low_memory=False, chunksize=500_000):
                frames.append(chunk[chunk["noteId"].isin(note_ids)])
    return pd.concat(frames, ignore_index=True).drop_duplicates("noteId")


# ── Caches ───────────────────────────────────────────────────────────────────

class JsonlCache:
    """Append-only key -> value cache."""

    def __init__(self, path: Path):
        self.path = path
        self.data = {}
        self.lock = threading.Lock()
        if path.exists():
            for line in path.read_text().splitlines():
                try:
                    rec = json.loads(line)
                    self.data[rec["key"]] = rec["value"]
                except (json.JSONDecodeError, KeyError):
                    pass

    def get(self, key, default=None):
        return self.data.get(key, default)

    def __contains__(self, key):
        return key in self.data

    def put(self, key, value):
        with self.lock:
            self.data[key] = value
            with open(self.path, "a") as f:
                f.write(json.dumps({"key": key, "value": value}) + "\n")


# ── Step 3: X posts ──────────────────────────────────────────────────────────

def fetch_x_posts(df, cfg, cache: JsonlCache, counts):
    from library.x_posts import fetch_posts

    token = os.environ.get("X_BEARER_TOKEN")
    if not token:
        sys.exit("X_BEARER_TOKEN is not set. Set it, or pass --stub-x for a test build.")

    ids = sorted(set(df.loc[df["drop_reason"].isna(), "tweetId"].astype(str)))
    todo = [i for i in ids if i not in cache]
    log(f"X posts: {len(ids) - len(todo)} cached, {len(todo)} to fetch")
    found, failed = fetch_posts(todo, token, cfg)
    for tid in todo:
        if tid in found:
            cache.put(tid, found[tid])
        elif tid not in failed:
            cache.put(tid, None)  # the API named it unavailable (deleted/protected/suspended)
    counts["x_batch_failures"] = len(failed)

    posts = {tid: cache.get(tid) for tid in ids}
    df = df.copy()
    df["x_post_text"] = df["tweetId"].map(lambda t: (posts.get(t) or {}).get("text"))
    df["x_post_lang"] = df["tweetId"].map(lambda t: (posts.get(t) or {}).get("lang"))
    df["x_post_created_at"] = df["tweetId"].map(lambda t: (posts.get(t) or {}).get("created_at"))
    df["tweet_has_media"] = df["tweetId"].map(lambda t: (posts.get(t) or {}).get("tweet_has_media"))
    df["tweet_media_type"] = df["tweetId"].map(lambda t: (posts.get(t) or {}).get("tweet_media_type"))
    df["x_post_retrieved"] = df["tweetId"].map(lambda t: posts.get(t) is not None)
    return df


def stub_x_posts(df):
    df = df.copy()
    df["x_post_text"] = ""
    df["x_post_lang"] = None
    df["x_post_created_at"] = None
    df["tweet_has_media"] = None
    df["tweet_media_type"] = None
    df["x_post_retrieved"] = True
    return df


# ── Steps 5-6: dependence filters ────────────────────────────────────────────
# Calling convention copied from End-to-End Precision Evaluation.ipynb
# (build_filter_user_content / call_filter_model), model swapped to gpt-6.1-sol.

def post_date_from_snowflake(tweet_id: str) -> str:
    try:
        ms = (int(tweet_id) >> 22) + 1288834974657
    except (TypeError, ValueError):
        return "N/A"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%d %B %Y")


def build_filter_user_content(row) -> str:
    has_media = row.get("tweet_has_media")
    has_media_str = "yes" if has_media == 1 else ("no" if has_media == 0 else "N/A")
    media_type = row.get("tweet_media_type")
    media_type_str = str(media_type) if pd.notna(media_type) and media_type else "N/A"
    return (
        f"<post_metadata>\n"
        f"post_date: {row.get('post_date', 'N/A')}\n"
        f"tweet_has_media: {has_media_str}\n"
        f"tweet_media_type: {media_type_str}\n"
        f"</post_metadata>\n\n"
        f"<post_text>\n{row['x_post_text']}\n</post_text>\n\n"
        f"<context_note>\n{row['summary']}\n</context_note>"
    )


def make_filter_caller(client, cfg):
    import openai
    from pydantic import BaseModel

    class BinaryLabel(BaseModel):
        label: Literal[0, 1]

    fcfg = cfg["models"]["dependence_filters"]

    def call_filter_model(instructions, user_content):
        max_retries = fcfg["max_attempts"]
        for attempt in range(max_retries):
            try:
                response = client.responses.parse(
                    model=fcfg["model"],
                    reasoning={"effort": fcfg["reasoning_effort"]},
                    instructions=instructions,
                    input=[{"role": "user", "content": user_content}],
                    text_format=BinaryLabel,
                )
                return response.output_parsed.label
            except openai.RateLimitError:
                wait = 20 * (attempt + 1)
                log(f"  429 rate limit -- sleeping {wait}s (attempt {attempt + 1})")
                time.sleep(wait)
            except Exception as e:
                if attempt == max_retries - 1:
                    log(f"  failed: {e}")
                    return None
                time.sleep(2)
        return None

    return call_filter_model


def run_filter(task, prompt_file, df, call, cfg, cache: JsonlCache):
    """noteId -> 0/1/None (None = failed after retries). Cached per
    (task, model, prompt, input)."""
    instructions = load_prompt(prompt_file).strip()
    model = cfg["models"]["dependence_filters"]["model"]
    prompt_hash = sha(instructions)[:12]

    keyed = {}
    for _, row in df.iterrows():
        content = build_filter_user_content(row)
        keyed[row["noteId"]] = (f"{task}|{model}|{prompt_hash}|{sha(content)[:16]}", content)

    todo = [(nid, k, c) for nid, (k, c) in keyed.items() if k not in cache]
    log(f"{task}: {len(keyed) - len(todo)} cached, {len(todo)} to label with {model}")
    done = 0
    with ThreadPoolExecutor(max_workers=cfg["models"]["dependence_filters"]["concurrency"]) as ex:
        futures = {ex.submit(call, instructions, c): (nid, k) for nid, k, c in todo}
        for fut in as_completed(futures):
            nid, k = futures[fut]
            label = fut.result()
            if label is not None:
                cache.put(k, label)  # failures aren't cached, so they retry next build
            done += 1
            if done % max(1, len(todo) // 20) == 0:
                log(f"  {task}: {done}/{len(todo)}")
    return {nid: cache.get(k) for nid, (k, _) in keyed.items()}


# ── Step 7: embeddings ───────────────────────────────────────────────────────

def embed_documents(client, docs, cfg, cache_path: Path):
    """L2-normalised embeddings for docs; reuses cached vectors by text hash.
    The cache is rewritten with only this build's documents (it rolls with
    the window)."""
    ecfg = cfg["models"]["embedding"]
    keys = [sha(ecfg["model"] + "\n" + d) for d in docs]
    cached = {}
    if cache_path.exists():
        with np.load(cache_path, allow_pickle=False) as z:
            cached = dict(zip(z["keys"].tolist(), z["vectors"]))

    todo_idx = [i for i, k in enumerate(keys) if k not in cached]
    log(f"embeddings: {len(docs) - len(todo_idx)} cached, {len(todo_idx)} to embed with {ecfg['model']}")
    if todo_idx:
        new = embed_openai(client, [docs[i] for i in todo_idx], ecfg["model"],
                           batch_size=ecfg["batch_size"], max_retries=ecfg["max_attempts"])
        for i, v in zip(todo_idx, new):
            cached[keys[i]] = v

    vectors = np.stack([cached[k] for k in keys]).astype(np.float32) if keys else np.zeros((0, 3072), np.float32)
    keep = sorted(set(keys))
    np.savez(cache_path, keys=np.array(keep), vectors=np.stack([cached[k] for k in keep]) if keep
             else np.zeros((0, vectors.shape[1]), np.float32))
    return l2_normalise(vectors) if len(vectors) else vectors


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cn-dir", required=True, type=Path,
                    help="Community Notes export: notes-*.zip|tsv and noteStatusHistory*.zip|tsv")
    ap.add_argument("--export-date", help="YYYY-MM-DD (default: date of the noteStatusHistory file)")
    ap.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    ap.add_argument("--stub-x", action="store_true",
                    help="No X API: empty post text, no English or dependence filters. Output is marked STUB.")
    ap.add_argument("--sample", type=int,
                    help="Use a random sample of N window notes (drawn before the X fetch, so the\n"
                         "library ends up smaller after the filters)")
    ap.add_argument("--seed", type=int, default=0, help="Random seed for --sample (default 0)")
    ap.add_argument("--include-note-ids", default="",
                    help="Comma-separated note IDs to add by hand. They skip the window and --sample and\n"
                         "are kept even if a dependence filter flags them (the labels are still recorded);\n"
                         "they still need a retrievable English X post and must be Helpful now.")
    ap.add_argument("--force", action="store_true", help="Overwrite an existing snapshot folder")
    args = ap.parse_args()

    load_env_file(REPO_ROOT / ".env")
    cfg = load_config()
    window_days = int(cfg["library"]["LIBRARY_WINDOW_DAYS"])

    export_date = (datetime.strptime(args.export_date, "%Y-%m-%d").date()
                   if args.export_date else export_date_of(args.cn_dir))
    window_end = datetime(export_date.year, export_date.month, export_date.day, tzinfo=timezone.utc)
    window_start = window_end - timedelta(days=window_days)

    include_ids = frozenset(i.strip() for i in args.include_note_ids.split(",") if i.strip())
    name = (export_date.isoformat() + ("-STUB" if args.stub_x else "")
            + (f"-sample{args.sample}" if args.sample else "")
            + (f"-plus{len(include_ids)}" if include_ids else ""))
    out_dir = args.out_root / name
    if out_dir.exists() and not args.force:
        sys.exit(f"{out_dir} already exists (use --force to overwrite)")
    cache_dir = args.out_root / "_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    if not os.environ.get("OPENAI_API_KEY"):
        sys.exit("OPENAI_API_KEY is not set (env or .env)")
    from openai import OpenAI
    client = OpenAI()

    counts = {}
    log(f"export date {export_date}; window {window_start:%Y-%m-%d} to {window_end:%Y-%m-%d} "
        f"({window_days} days, end exclusive)")

    # 1. Helpful notes first rated Helpful inside the window
    status, picked, counts["status_rows"] = load_window(
        args.cn_dir, int(window_start.timestamp() * 1000), int(window_end.timestamp() * 1000), include_ids)
    picked = picked[~picked["noteId"].isin(status["noteId"])]  # already in the window
    counts["helpful_first_helpful_in_window"] = len(status)
    log(f"{len(status)} notes Helpful now and first rated Helpful in the window")

    # 2. Note text
    notes = load_notes(args.cn_dir, set(status["noteId"]) | set(picked["noteId"]))
    df = status.merge(notes, on="noteId", how="left")
    df["drop_reason"] = None
    df.loc[df["summary"].isna() | (df["summary"].astype(str).str.strip() == ""), "drop_reason"] = "no_summary"
    df["summary"] = df["summary"].fillna("").astype(str)
    df["tweetId"] = df["tweetId"].astype(str)
    counts["with_summary"] = int(df["drop_reason"].isna().sum())

    if args.sample:
        live = df[df["drop_reason"].isna()]
        sample = live.sample(n=min(args.sample, len(live)), random_state=args.seed).index
        df = df.loc[sample].reset_index(drop=True)
        counts["random_sample"] = len(df)
        log(f"--sample: {len(df)} of {len(live)} notes (seed {args.seed})")

    df["hand_picked"] = df["noteId"].isin(include_ids)
    if len(picked):
        extra = picked.merge(notes, on="noteId", how="left")
        extra["drop_reason"] = None
        extra.loc[extra["summary"].isna() | (extra["summary"].astype(str).str.strip() == ""), "drop_reason"] = "no_summary"
        extra["summary"] = extra["summary"].fillna("").astype(str)
        extra["tweetId"] = extra["tweetId"].astype(str)
        extra["hand_picked"] = True
        df = pd.concat([df, extra], ignore_index=True)
        log(f"added {len(extra)} hand-picked note(s): {', '.join(extra['noteId'])}")
    counts["hand_picked"] = int(df["hand_picked"].sum())

    def alive():
        return df["drop_reason"].isna()

    # 3. Original X post
    if args.stub_x:
        log("STUB: skipping X API (empty post text, no English filter)")
        df = stub_x_posts(df)
    else:
        df = fetch_x_posts(df, cfg, JsonlCache(cache_dir / "x_posts.jsonl"), counts)
    df.loc[alive() & ~df["x_post_retrieved"], "drop_reason"] = "x_post_unavailable"
    counts["x_post_retrieved"] = int(alive().sum())

    # 4. English (X post lang)
    if not args.stub_x:
        df.loc[alive() & (df["x_post_lang"] != cfg["library"]["english_lang"]), "drop_reason"] = "not_english"
    counts["english"] = int(alive().sum()) if not args.stub_x else None
    df["post_date"] = df["tweetId"].map(post_date_from_snowflake)

    # 5-6. Dependence filters. Skipped for a stub build: without the post
    # text the media filter flags every note (checked against the
    # evaluation's labels), so stub output would be meaningless.
    if args.stub_x:
        log("STUB: skipping the dependence filters (they need the X post text)")
        df["author_dependent"] = None
        df["media_dependent"] = None
        counts["not_author_dependent"] = counts["not_media_dependent"] = None
    else:
        call = make_filter_caller(client, cfg)
        filter_cache = JsonlCache(cache_dir / "filters.jsonl")

        author = run_filter("author_dependence", "author_dependence.txt", df[alive()], call, cfg, filter_cache)
        df["author_dependent"] = df["noteId"].map(author)
        df.loc[alive() & df["author_dependent"].isna(), "drop_reason"] = "author_filter_failed"
        df.loc[alive() & (df["author_dependent"] == 1) & ~df["hand_picked"], "drop_reason"] = "author_dependent"
        counts["not_author_dependent"] = int(alive().sum())

        media = run_filter("media_dependence", "media_dependence.txt", df[alive()], call, cfg, filter_cache)
        df["media_dependent"] = df["noteId"].map(media)
        df.loc[alive() & df["media_dependent"].isna(), "drop_reason"] = "media_filter_failed"
        df.loc[alive() & (df["media_dependent"] == 1) & ~df["hand_picked"], "drop_reason"] = "media_dependent"
        counts["not_media_dependent"] = int(alive().sum())

    # 7-8. Documents, embeddings, TF-IDF
    kept = df[alive()].sort_values("noteId").reset_index(drop=True)
    kept["document"] = [note_document(x, s) for x, s in zip(kept["x_post_text"], kept["summary"])]
    if kept.empty:
        sys.exit("No notes survived the filters; nothing to build.")
    embeddings = embed_documents(client, kept["document"].tolist(), cfg, cache_dir / "embeddings.npz")
    vectorizer, tfidf_matrix = build_tfidf_index(kept["document"].tolist())
    counts["library_notes"] = len(kept)
    counts["drop_reasons"] = df["drop_reason"].value_counts().to_dict()

    # Write
    tmp_dir = out_dir.with_name(out_dir.name + ".tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    note_cols = ["noteId", "tweetId", "hand_picked", "summary", "classification", "createdAtMillis", "firstHelpfulMillis",
                 "x_post_text", "x_post_lang", "x_post_created_at", "tweet_has_media", "tweet_media_type",
                 "post_date", "document"]
    kept[note_cols].to_parquet(tmp_dir / "notes.parquet", index=False)
    np.save(tmp_dir / "embeddings.npy", embeddings)
    with open(tmp_dir / "tfidf_vectorizer.pkl", "wb") as f:
        pickle.dump(vectorizer, f)
    with open(tmp_dir / "tfidf_matrix.pkl", "wb") as f:
        pickle.dump(tfidf_matrix, f)
    cand_cols = ["noteId", "tweetId", "hand_picked", "firstHelpfulMillis", "summary", "x_post_retrieved", "x_post_lang",
                 "author_dependent", "media_dependent", "drop_reason"]
    df[cand_cols].to_parquet(tmp_dir / "candidates.parquet", index=False)

    import sklearn
    manifest = {
        "name": name,
        "built_at": datetime.now(timezone.utc).isoformat(),
        "stub_x_posts": bool(args.stub_x),
        "sample": args.sample,
        "sample_seed": args.seed if args.sample else None,
        "hand_picked_note_ids": sorted(include_ids),
        "export_date": export_date.isoformat(),
        "cn_dir": str(args.cn_dir.resolve()),
        "window": {"days": window_days, "start_utc": window_start.isoformat(),
                   "end_utc_exclusive": window_end.isoformat(),
                   "first_helpful_min": pd.to_datetime(df["firstHelpfulMillis"].min(), unit="ms", utc=True).isoformat()
                   if len(df) else None,
                   "first_helpful_max": pd.to_datetime(df["firstHelpfulMillis"].max(), unit="ms", utc=True).isoformat()
                   if len(df) else None},
        "counts": counts,
        "models": {"dependence_filters": cfg["models"]["dependence_filters"]["model"],
                   "dependence_reasoning_effort": cfg["models"]["dependence_filters"]["reasoning_effort"],
                   "embedding": cfg["models"]["embedding"]["model"]},
        "prompt_sha256": {p: sha(load_prompt(p)) for p in
                          ["author_dependence.txt", "media_dependence.txt", "applicability.txt"]},
        "tfidf_vocab_size": len(vectorizer.vocabulary_),
        "embedding_shape": list(embeddings.shape),
        "versions": {"python": sys.version.split()[0], "scikit-learn": sklearn.__version__,
                     "numpy": np.__version__, "pandas": pd.__version__},
    }
    (tmp_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

    if out_dir.exists():
        import shutil
        shutil.rmtree(out_dir)
    tmp_dir.rename(out_dir)

    log(f"wrote {out_dir}")
    for k, v in counts.items():
        log(f"  {k}: {v}")


if __name__ == "__main__":
    main()

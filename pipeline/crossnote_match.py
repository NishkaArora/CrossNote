"""Live matching for one Bluesky post against the note library.

    length filter  -> drop posts with <= 2 words (whitespace split)
    Stage 1 lexical   max unigram TF-IDF cosine to any note >= threshold
    Stage 1 embedding max text-embedding-3-large cosine to any note >= threshold
    Stage 2        top 3 notes by that cosine (reuses the Stage 1 embedding)
    Stage 3        gpt-6.1-sol applicability judgment on each of the 3 pairs;
                   the highest-cosine applicable note is the match

Thresholds come from configs/thresholds.yaml; models and settings from
configs/pipeline.yaml. The Stage 3 prompt, messages, schema and retry loop are
copied from the evaluation (notebook 04, call_stage3_model).

The library is the snapshot folder named in <LIBRARY_ROOT>/CURRENT. The
matcher checks that file every LIBRARY_CHECK_SECONDS and swaps in a new
snapshot when it changes, so a daily rebuild needs no restart.
"""
from __future__ import annotations

import html
import json
import os
import pickle
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from library.common import embed_openai, l2_normalise, load_config, load_prompt, tfidf_similarities

LIBRARY_ROOT = Path(os.environ.get("LIBRARY_ROOT", "/data/library"))
LIBRARY_CHECK_SECONDS = int(os.environ.get("LIBRARY_CHECK_SECONDS", "300"))
STAGE3_LOG_PATH = os.environ.get("STAGE3_LOG_PATH", "/data/crossnote_stage3.jsonl")
ALLOW_STUB = os.environ.get("CROSSNOTE_ALLOW_STUB_LIBRARY") == "1"

STAGE3_RESPONSE_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "applicability_judgment",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "applicable": {"type": "boolean"},
                "reason": {"type": "string"},
            },
            "required": ["applicable", "reason"],
            "additionalProperties": False,
        },
    },
}


def build_stage3_messages(prompt, post_text, note_text, tweet_text):
    return [
        {"role": "system", "content": prompt},
        {"role": "user",
         "content": f"<original_post>{tweet_text}</original_post>\n<note>{note_text}</note>\n<new_post>{post_text}</new_post>"},
    ]


class Library:
    def __init__(self, path: Path):
        self.path = path
        self.manifest = json.loads((path / "manifest.json").read_text())
        if self.manifest.get("stub_x_posts") and not ALLOW_STUB:
            raise RuntimeError(f"{path.name} is a STUB build (no X posts); refusing to load it. "
                               "Set CROSSNOTE_ALLOW_STUB_LIBRARY=1 only for local testing.")
        self.notes = pd.read_parquet(path / "notes.parquet")
        self.embeddings = np.load(path / "embeddings.npy")
        with open(path / "tfidf_vectorizer.pkl", "rb") as f:
            self.vectorizer = pickle.load(f)
        with open(path / "tfidf_matrix.pkl", "rb") as f:
            self.tfidf_matrix = pickle.load(f)
        n = len(self.notes)
        if self.embeddings.shape[0] != n or self.tfidf_matrix.shape[0] != n:
            raise RuntimeError(f"{path.name}: notes ({n}), embeddings ({self.embeddings.shape[0]}) and "
                               f"TF-IDF rows ({self.tfidf_matrix.shape[0]}) don't line up")
        self.name = path.name


def current_library_name():
    pointer = LIBRARY_ROOT / "CURRENT"
    return pointer.read_text().strip() if pointer.exists() else None


class Matcher:
    def __init__(self):
        from openai import OpenAI

        self.cfg = load_config()
        self.client = OpenAI() if os.environ.get("OPENAI_API_KEY") else None
        self.prompt = load_prompt("applicability.txt")
        self.library: Library | None = None
        self.library_error: str | None = None
        self._loaded_name = None
        self._last_check = 0.0
        self._lock = threading.Lock()
        self._log_lock = threading.Lock()
        self._pool = ThreadPoolExecutor(max_workers=self.cfg["stage2"]["top_k"] * 4)
        self.stats = {"received": 0, "length_dropped": 0, "passed_tfidf": 0, "passed_embedding": 0,
                      "stage3_calls": 0, "stage3_errors": 0, "matched": 0, "errors": 0}
        self.maybe_reload(force=True)

    # ── library ──────────────────────────────────────────────────────────────

    def maybe_reload(self, force=False):
        now = time.time()
        if not force and now - self._last_check < LIBRARY_CHECK_SECONDS:
            return
        with self._lock:
            self._last_check = now
            name = current_library_name()
            if name is None:
                self.library_error = f"no {LIBRARY_ROOT / 'CURRENT'} file"
                print(f"[matcher] {self.library_error}; matching disabled")
                return
            if name == self._loaded_name:
                return
            try:
                lib = Library(LIBRARY_ROOT / name)
            except Exception as e:
                self.library_error = f"failed to load {name}: {e}"
                self._loaded_name = name  # don't retry a broken snapshot every request
                print(f"[matcher] {self.library_error}"
                      + (f"; still using {self.library.name}" if self.library else ""))
                return
            self.library, self._loaded_name, self.library_error = lib, name, None
            print(f"[matcher] loaded library {name}: {len(lib.notes)} notes "
                  f"(export {lib.manifest.get('export_date')})")

    def is_ready(self):
        return self.library is not None and self.client is not None

    # ── Stage 3 ──────────────────────────────────────────────────────────────

    def call_stage3_model(self, post_text, note_text, tweet_text):
        scfg = self.cfg["models"]["stage3_applicability"]
        max_retries = scfg["max_attempts"]
        for attempt in range(max_retries):
            try:
                resp = self.client.chat.completions.create(
                    model=scfg["model"],
                    messages=build_stage3_messages(self.prompt, post_text, note_text, tweet_text),
                    response_format=STAGE3_RESPONSE_SCHEMA,
                )
                content = resp.choices[0].message.content
                if content is None:
                    raise ValueError("model returned no content (possible refusal)")
                return json.loads(content)
            except Exception:
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise

    def _judge(self, post_text, row, rank, cosine, tfidf):
        judgment = {"rank": rank, "note_id": row["noteId"], "tweet_id": row["tweetId"],
                    "cosine": cosine, "tfidf": tfidf, "applicable": False, "reason": None, "error": None}
        self.stats["stage3_calls"] += 1
        try:
            out = self.call_stage3_model(post_text, row["summary"], row["x_post_text"])
            judgment["applicable"] = out["applicable"] is True
            judgment["reason"] = out["reason"]
        except Exception as e:
            self.stats["stage3_errors"] += 1
            judgment["error"] = f"{type(e).__name__}: {e}"[:300]
        return judgment

    # ── one post ─────────────────────────────────────────────────────────────

    def match(self, text: str, uri: str | None = None) -> dict:
        self.maybe_reload()
        lib = self.library
        self.stats["received"] += 1
        result = {"stage": None, "match": None, "library": lib.name if lib else None}
        if lib is None or self.client is None:
            result["stage"] = "not_ready"
            result["error"] = self.library_error or "OPENAI_API_KEY not set"
            return result

        s1 = self.cfg["stage1"]
        if len(text.split()) <= s1["min_words_exclusive"]:
            self.stats["length_dropped"] += 1
            result["stage"] = "length"
            return result

        tfidf = tfidf_similarities(text, lib.vectorizer, lib.tfidf_matrix)
        result["tfidf_max"] = float(tfidf.max())
        if result["tfidf_max"] < s1["tfidf_threshold"]:
            result["stage"] = "tfidf"
            return result
        self.stats["passed_tfidf"] += 1

        ecfg = self.cfg["models"]["embedding"]
        query = embed_openai(self.client, [text], ecfg["model"], max_retries=ecfg["max_attempts"])
        cosine = lib.embeddings @ l2_normalise(query)[0]
        result["cosine_max"] = float(cosine.max())
        if result["cosine_max"] < s1["embedding_threshold"]:
            result["stage"] = "embedding"
            return result
        self.stats["passed_embedding"] += 1

        top = np.argsort(-cosine, kind="stable")[: self.cfg["stage2"]["top_k"]]
        futures = [self._pool.submit(self._judge, text, lib.notes.iloc[i], rank, float(cosine[i]), float(tfidf[i]))
                   for rank, i in enumerate(top, 1)]
        judgments = [f.result() for f in futures]  # rank order = cosine order
        result["stage"] = "stage3"
        result["candidates"] = judgments

        best = next((j for j in judgments if j["applicable"]), None)
        if best:
            self.stats["matched"] += 1
            row = lib.notes.iloc[top[best["rank"] - 1]]
            result["match"] = {
                **best,
                "note_summary": row["summary"],
                "reply_note_text": html.unescape(row["summary"]),
                "x_post_text": row["x_post_text"],
            }
        self._log_stage3(uri, text, result)
        return result

    def _log_stage3(self, uri, text, result):
        rec = {"ts": datetime.now(timezone.utc).isoformat(), "uri": uri, "post_text": text,
               "library": result["library"], "tfidf_max": result.get("tfidf_max"),
               "cosine_max": result.get("cosine_max"), "candidates": result.get("candidates"),
               "matched_note_id": (result["match"] or {}).get("note_id")}
        try:
            with self._log_lock, open(STAGE3_LOG_PATH, "a") as f:
                f.write(json.dumps(rec) + "\n")
        except OSError as e:
            print(f"[matcher] could not write {STAGE3_LOG_PATH}: {e}")

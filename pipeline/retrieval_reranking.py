"""
Three-stage pipeline over Community Notes.

Stage 1 — IDF-normalized BM25 pre-filter (~3ms)
  Keeps the top-5 notes if best_score / idf_sum >= BM25_CUTOFF.

Stage 2 — Cross-encoder reranking (~50ms on 5 candidates)
  Reranks the top-5 BM25 candidates. Drops posts whose best CE score
  is below CE_CUTOFF.

Stage 3 — LLM verification (~500ms per call)
  Calls the LLM on each note in ranked order; returns on first "Yes".
  Skipped if OPENAI_API_KEY is not set.
"""

import os
import pickle
import time
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from nltk.tokenize import TweetTokenizer
from openai import OpenAI
from rank_bm25 import BM25Okapi
from sentence_transformers import CrossEncoder

NOTES_PATH  = Path(os.getenv("NOTES_PATH",  "/data/cn_crh_notes.tsv"))
BM25_PATH   = Path(os.getenv("BM25_PATH",   "/data/bm25_index.pkl"))
MODEL_CACHE = Path(os.getenv("SENTENCE_TRANSFORMERS_HOME", "/data/model_cache"))

os.environ.setdefault("HF_HOME", str(MODEL_CACHE))

BM25_CUTOFF = float(os.getenv("BM25_CUTOFF", "1.0"))
CE_CUTOFF   = float(os.getenv("CE_CUTOFF",   "2.0"))
BM25_TOP_K  = 5
LLM_MODEL   = os.getenv("LLM_MODEL", "gpt-4o-mini")

_SYSTEM_PROMPT = (
    "You are checking whether a Community Note applies to a Bluesky post. "
    "A note applies if the post directly makes or shares the specific claim the note fact-checks. "
    "Answer with a single word: Yes or No."
)

_tokenizer = TweetTokenizer(preserve_case=False, strip_handles=False, reduce_len=False)
_openai    = OpenAI(api_key=os.getenv("OPENAI_API_KEY")) if os.getenv("OPENAI_API_KEY") else None

_ready:         bool                   = False
_notes:         list[str]              = []
_bm25:          Optional[BM25Okapi]    = None
_cross_encoder: Optional[CrossEncoder] = None

stats = {"received": 0, "passed_bm25": 0, "passed_ce": 0, "passed_llm": 0}


def _tokenize(text: str) -> list[str]:
    return _tokenizer.tokenize(text) if isinstance(text, str) else []


def _llm_applies(post: str, note: str) -> bool:
    response = _openai.chat.completions.create(
        model=LLM_MODEL,
        max_tokens=5,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user",   "content": f"Post: {post}\n\nNote: {note}\n\nDoes this note apply?"},
        ],
    )
    return (response.choices[0].message.content or "").strip().lower().startswith("yes")


def is_ready() -> bool:
    return _ready


def load() -> None:
    global _ready, _notes, _bm25, _cross_encoder

    if not NOTES_PATH.exists():
        raise FileNotFoundError(
            f"Notes not found at {NOTES_PATH}. "
            "Run data/filter_notes.py then upload with fly sftp."
        )

    print(f"Loading notes from {NOTES_PATH}...")
    _notes = (
        pd.read_csv(NOTES_PATH, sep="\t", dtype=str)["summary"]
        .fillna("").astype(str).tolist()
    )
    print(f"  {len(_notes):,} notes")

    if BM25_PATH.exists():
        print(f"Loading BM25 index from {BM25_PATH}...")
        with open(BM25_PATH, "rb") as f:
            _bm25 = pickle.load(f)
    else:
        print("BM25 index not found — building from scratch...")
        _bm25 = BM25Okapi([_tokenize(s) for s in _notes])

    print("Loading cross-encoder (ms-marco-MiniLM-L-6-v2)...")
    _cross_encoder = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2", cache_folder=str(MODEL_CACHE))

    _ready = True
    print("Pipeline ready.")


def analyze(text: str) -> Optional[dict]:
    if not _ready:
        return None

    tokens = _tokenize(text)
    stats["received"] += 1

    # ── Stage 1: IDF-normalized BM25 pre-filter ──────────────────────────────
    bm25_scores = np.array(_bm25.get_scores(tokens))
    idf_sum = sum(_bm25.idf.get(t, 0.0) for t in tokens)
    if idf_sum == 0 or float(bm25_scores.max()) / idf_sum < BM25_CUTOFF:
        return None

    stats["passed_bm25"] += 1

    # ── Stage 2: Cross-encoder reranking ─────────────────────────────────────
    top_indices = bm25_scores.argsort()[::-1][:BM25_TOP_K].tolist()
    ce_scores   = _cross_encoder.predict([[text, _notes[i]] for i in top_indices])
    ranked      = sorted(zip(ce_scores, top_indices), key=lambda x: x[0], reverse=True)

    if float(ranked[0][0]) < CE_CUTOFF:
        return None

    stats["passed_ce"] += 1

    # ── Stage 3: LLM verification ─────────────────────────────────────────────
    if _openai is None:
        best_score, best_idx = ranked[0]
        return {"note": _notes[best_idx], "score": float(best_score)}

    for ce_score, idx in ranked:
        if _llm_applies(text, _notes[idx]):
            stats["passed_llm"] += 1
            return {"note": _notes[idx], "score": float(ce_score)}

    return None


def debug_analyze(text: str) -> dict:
    result: dict = {"text": text, "stages": {}}

    tokens = _tokenize(text)

    # Stage 1
    t0 = time.perf_counter()
    bm25_scores = np.array(_bm25.get_scores(tokens))
    idf_sum = sum(_bm25.idf.get(t, 0.0) for t in tokens)
    bm25_norm = float(bm25_scores.max()) / idf_sum if idf_sum > 0 else 0.0
    bm25_ms = (time.perf_counter() - t0) * 1000

    top_indices = bm25_scores.argsort()[::-1][:BM25_TOP_K].tolist()
    result["stages"]["bm25"] = {
        "ms": round(bm25_ms, 1),
        "normalized_score": round(bm25_norm, 4),
        "cutoff": BM25_CUTOFF,
        "passed": bm25_norm >= BM25_CUTOFF,
        "top_candidates": [
            {"idx": i, "score": round(float(bm25_scores[i]), 3), "note": _notes[i][:80]}
            for i in top_indices
        ],
    }

    if bm25_norm < BM25_CUTOFF:
        return result

    # Stage 2
    t1 = time.perf_counter()
    ce_scores = _cross_encoder.predict([[text, _notes[i]] for i in top_indices])
    ce_ms = (time.perf_counter() - t1) * 1000
    ranked = sorted(zip(ce_scores, top_indices), key=lambda x: x[0], reverse=True)
    best_ce_score = float(ranked[0][0])

    result["stages"]["cross_encoder"] = {
        "ms": round(ce_ms, 1),
        "best_score": round(best_ce_score, 4),
        "cutoff": CE_CUTOFF,
        "passed": best_ce_score >= CE_CUTOFF,
        "ranked": [
            {"score": round(float(s), 4), "note": _notes[i][:80]}
            for s, i in ranked
        ],
    }

    if best_ce_score < CE_CUTOFF:
        return result

    # Stage 3
    if _openai is None:
        result["stages"]["llm"] = {"skipped": "no OPENAI_API_KEY"}
        return result

    llm_results = []
    for ce_score, idx in ranked:
        t2 = time.perf_counter()
        applies = _llm_applies(text, _notes[idx])
        llm_ms = (time.perf_counter() - t2) * 1000
        llm_results.append({
            "note": _notes[idx][:80],
            "applies": applies,
            "ms": round(llm_ms, 1),
        })
        if applies:
            break

    result["stages"]["llm"] = {"calls": llm_results}
    return result

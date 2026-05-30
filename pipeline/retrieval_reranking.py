"""
Three-stage retrieval and reranking pipeline over Community Notes.

Stage 1 — BM25 + dense pre-filter (runs on every post, ~5ms)
  Top-1 BM25 score and top-1 cosine similarity are checked against 95th-percentile
  thresholds. Posts below both thresholds are dropped immediately.

Stage 2 — cross-encoder reranking (runs on ~10% of posts, ~50ms)
  Candidates from BM25 top-10, dense top-10, and combined top-30 are reranked
  by cross-encoder score. Drops posts whose best score is below CE_CUTOFF.

Stage 3 — LLM verification (runs only on Stage 2 survivors, ~500ms)
  Asks the LLM whether the top Community Note directly applies to the post.
  Skipped if OPENAI_API_KEY is not set.

Startup loads pre-computed BM25 index and note embeddings from disk (fast).
Falls back to computing from scratch if the files are missing (slow, ~5 min).
"""

import os
import pickle
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from nltk.tokenize import TweetTokenizer
from openai import OpenAI
from rank_bm25 import BM25Okapi
from sentence_transformers import CrossEncoder, SentenceTransformer

NOTES_PATH  = Path(os.getenv("NOTES_PATH",   "/data/cn_crh_notes.tsv"))
BM25_PATH   = Path(os.getenv("BM25_PATH",    "/data/bm25_index.pkl"))
EMB_PATH    = Path(os.getenv("EMB_PATH",     "/data/note_embeddings.npy"))
MODEL_CACHE = Path(os.getenv("SENTENCE_TRANSFORMERS_HOME", "/data/model_cache"))

BM25_CUTOFF  = float(os.getenv("BM25_CUTOFF",  "58.913"))
DENSE_CUTOFF = float(os.getenv("DENSE_CUTOFF", "0.542"))
CE_CUTOFF    = float(os.getenv("CE_CUTOFF",    "2.0"))
LLM_MODEL    = os.getenv("LLM_MODEL", "gpt-4o-mini")

_SYSTEM_PROMPT = (
    "You are checking whether a Community Note applies to a Bluesky post. "
    "A note applies if the post directly makes or shares the specific claim the note fact-checks. "
    "Answer with a single word: Yes or No."
)

_tokenizer = TweetTokenizer(preserve_case=False, strip_handles=False, reduce_len=False)
_openai    = OpenAI(api_key=os.getenv("OPENAI_API_KEY")) if os.getenv("OPENAI_API_KEY") else None

_ready:           bool                          = False
_notes:           list[str]                     = []
_bm25:            Optional[BM25Okapi]           = None
_dense_model:     Optional[SentenceTransformer] = None
_note_embeddings: Optional[np.ndarray]          = None
_cross_encoder:   Optional[CrossEncoder]        = None


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
    global _ready, _notes, _bm25, _dense_model, _note_embeddings, _cross_encoder

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
        print("BM25 index not found — building from scratch (run data/precompute.py to avoid this)...")
        _bm25 = BM25Okapi([_tokenize(s) for s in _notes])

    print("Loading dense encoder (all-MiniLM-L6-v2)...")
    _dense_model = SentenceTransformer(
        "sentence-transformers/all-MiniLM-L6-v2",
        cache_folder=str(MODEL_CACHE),
    )

    if EMB_PATH.exists():
        print(f"Loading note embeddings from {EMB_PATH}...")
        _note_embeddings = np.load(EMB_PATH)
    else:
        print("Note embeddings not found — computing from scratch (run data/precompute.py to avoid this)...")
        _note_embeddings = _dense_model.encode(
            _notes, batch_size=256, normalize_embeddings=True, show_progress_bar=True,
        )

    print("Loading cross-encoder (ms-marco-MiniLM-L-6-v2)...")
    _cross_encoder = CrossEncoder(
        "cross-encoder/ms-marco-MiniLM-L-6-v2",
        cache_folder=str(MODEL_CACHE),
    )

    _ready = True
    print("Pipeline ready.")


def analyze(text: str) -> Optional[dict]:
    if not _ready:
        return None

    tokens = _tokenize(text)

    # ── Stage 1: BM25 + dense pre-filter ─────────────────────────────────────
    bm25_scores = np.array(_bm25.get_scores(tokens))
    dense_sims  = _note_embeddings @ _dense_model.encode([text], normalize_embeddings=True)[0]

    if float(bm25_scores.max()) < BM25_CUTOFF and float(dense_sims.max()) < DENSE_CUTOFF:
        return None

    # ── Stage 2: cross-encoder reranking ─────────────────────────────────────
    n = len(_notes)

    bm25_ranks  = np.empty(n)
    bm25_ranks[bm25_scores.argsort()[::-1]]  = np.arange(1, n + 1)

    dense_ranks = np.empty(n)
    dense_ranks[dense_sims.argsort()[::-1]]  = np.arange(1, n + 1)

    candidates = list(
        set(bm25_scores.argsort()[::-1][:10].tolist())
        | set(dense_sims.argsort()[::-1][:10].tolist())
        | set(((1.0 / bm25_ranks) + (1.0 / dense_ranks)).argsort()[::-1][:30].tolist())
    )

    # Rerank candidates by cross-encoder score; keep the top result
    ce_scores  = _cross_encoder.predict([[text, _notes[i]] for i in candidates], batch_size=32)
    best_local = int(np.argmax(ce_scores))
    best_score = float(ce_scores[best_local])

    if best_score < CE_CUTOFF:
        return None

    best_note = _notes[candidates[best_local]]

    # ── Stage 3: LLM verification ─────────────────────────────────────────────
    if _openai is not None and not _llm_applies(text, best_note):
        return None

    return {"note": best_note, "score": best_score}

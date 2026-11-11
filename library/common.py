"""Shared by the library build (library/build.py) and the live matcher
(pipeline/crossnote_match.py): config loading, the note document text,
TF-IDF and text-embedding-3-large, all copied from the evaluation code
(released_artefacts notebooks 02/03, _score_stage1_methods) so the live
pipeline scores posts exactly as the evaluation did.
"""
from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = REPO_ROOT / "configs"
PROMPT_DIR = REPO_ROOT / "prompts"


def load_config() -> dict:
    """pipeline.yaml plus the Stage 1 thresholds from thresholds.yaml."""
    cfg = yaml.safe_load((CONFIG_DIR / "pipeline.yaml").read_text())
    thr = yaml.safe_load((CONFIG_DIR / "thresholds.yaml").read_text())
    s1 = thr["stage1_stream_filtering"]
    cfg["stage1"]["tfidf_threshold"] = float(s1["lexical_gate"]["threshold"])
    cfg["stage1"]["embedding_threshold"] = float(s1["embedding_gate"]["threshold"])
    cfg["stage2"]["top_k"] = int(thr["stage2_candidate_ranking"]["top_k"])
    return cfg


def load_prompt(name: str) -> str:
    """Prompt file contents. The evaluation read applicability.txt with
    f.read() (no strip) and the filter prompts with .strip()."""
    return (PROMPT_DIR / name).read_text()


def note_document(x_post_text: str, note_summary: str) -> str:
    """One document per note: X post text + " " + note summary. The same
    string feeds TF-IDF and text-embedding-3-large (notebook 02,
    _build_claim_corpus, for a single-note cluster)."""
    return " ".join([x_post_text or "", note_summary or ""])


# ── TF-IDF (verbatim settings from _score_stage1_methods) ────────────────────

def build_tfidf_index(corpus_texts):
    from sklearn.feature_extraction.text import TfidfVectorizer

    vectorizer = TfidfVectorizer(
        lowercase=True, stop_words=None, ngram_range=(1, 1),
        sublinear_tf=False, use_idf=True, smooth_idf=True, norm="l2",
    )
    corpus_matrix = vectorizer.fit_transform(corpus_texts)
    return vectorizer, corpus_matrix


def tfidf_similarities(query_text: str, vectorizer, corpus_matrix) -> np.ndarray:
    """Cosine similarity of one query to every corpus document
    (query @ corpus.T; rows are l2-normalised)."""
    query_matrix = vectorizer.transform([query_text])
    sims = query_matrix @ corpus_matrix.T
    return np.asarray(sims.todense()).ravel()


# ── text-embedding-3-large (verbatim from _score_stage1_methods) ─────────────

def embed_openai(client, texts, model_name, batch_size=100, max_retries=5):
    from openai import APIConnectionError, InternalServerError, RateLimitError

    vectors = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        for attempt in range(max_retries):
            try:
                resp = client.embeddings.create(model=model_name, input=batch)
                break
            except (RateLimitError, APIConnectionError, InternalServerError):
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)
        vectors.extend([item.embedding for item in resp.data])
    return np.array(vectors, dtype=np.float32)


def l2_normalise(vectors: np.ndarray) -> np.ndarray:
    return vectors / np.linalg.norm(vectors, axis=1, keepdims=True)

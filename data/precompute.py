"""
Pre-computes the BM25 index and dense note embeddings from the filtered notes TSV.
Run this locally after filter_notes.py, then upload all three files to Fly.

Usage:
  pip install -r pipeline/requirements.txt
  python data/precompute.py

Outputs:
  data/cleaned/bm25_index.pkl       (~1 MB)
  data/cleaned/note_embeddings.npy  (~13 MB for 8k notes × 384 dims)

Upload all three to Fly:
  fly sftp shell
  put data/cleaned/cn_crh_notes.tsv /data/cn_crh_notes.tsv
  put data/cleaned/bm25_index.pkl /data/bm25_index.pkl
  put data/cleaned/note_embeddings.npy /data/note_embeddings.npy
"""

import pickle
from pathlib import Path

import numpy as np
import pandas as pd
from nltk.tokenize import TweetTokenizer
from rank_bm25 import BM25Okapi
from sentence_transformers import SentenceTransformer

CLEANED_DIR = Path(__file__).parent / "cleaned"
NOTES_PATH  = CLEANED_DIR / "cn_crh_notes.tsv"

if not NOTES_PATH.exists():
    raise FileNotFoundError(f"{NOTES_PATH} not found. Run filter_notes.py first.")

print(f"Loading {NOTES_PATH}...")
notes = pd.read_csv(NOTES_PATH, sep="\t", dtype=str)["summary"].fillna("").astype(str).tolist()
print(f"  {len(notes):,} notes")

# ── BM25 ──────────────────────────────────────────────────────────────────────
print("\nBuilding BM25 index...")
tokenizer = TweetTokenizer(preserve_case=False, strip_handles=False, reduce_len=False)
bm25 = BM25Okapi([tokenizer.tokenize(s) for s in notes])

bm25_path = CLEANED_DIR / "bm25_index.pkl"
with open(bm25_path, "wb") as f:
    pickle.dump(bm25, f)
print(f"  Saved → {bm25_path}  ({bm25_path.stat().st_size / 1e6:.1f} MB)")

# ── Dense embeddings ──────────────────────────────────────────────────────────
print("\nLoading all-MiniLM-L6-v2...")
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

print("Encoding note embeddings...")
embeddings = model.encode(
    notes,
    batch_size=256,
    normalize_embeddings=True,
    show_progress_bar=True,
)

emb_path = CLEANED_DIR / "note_embeddings.npy"
np.save(emb_path, embeddings)
print(f"  Saved → {emb_path}  ({emb_path.stat().st_size / 1e6:.1f} MB, shape {embeddings.shape})")

print("\nDone. Upload to Fly with:")
print("  fly sftp shell")
print("  put data/cleaned/cn_crh_notes.tsv /data/cn_crh_notes.tsv")
print("  put data/cleaned/bm25_index.pkl /data/bm25_index.pkl")
print("  put data/cleaned/note_embeddings.npy /data/note_embeddings.npy")

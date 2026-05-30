"""
Filters raw Community Notes data down to a clean set for the CrossNote labeler.

Input:  data/raw/notes-*.zip + data/raw/noteStatusHistory-00000.zip
Output: data/cleaned/cn_crh_notes.tsv

Run download_notes.py first, then:
  pip install pandas langdetect
  python data/filter_notes.py
"""

import html
import re
import unicodedata
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from langdetect import DetectorFactory, LangDetectException, detect

DetectorFactory.seed = 0  # reproducible detection

RAW_DIR     = Path(__file__).parent / "raw"
CLEANED_DIR = Path(__file__).parent / "cleaned"
CLEANED_DIR.mkdir(exist_ok=True)

# Rolling 90-day window ending 2 days ago (Community Notes has a ~48h release lag)
COLLECTION_END  = datetime.now(timezone.utc) - timedelta(days=2)
WINDOW_START_MS = int((COLLECTION_END - timedelta(days=90)).timestamp() * 1000)
WINDOW_END_MS   = int(COLLECTION_END.timestamp() * 1000)

print(f"Window: {(COLLECTION_END - timedelta(days=90)).date()} → {COLLECTION_END.date()}")


# ── Load ──────────────────────────────────────────────────────────────────────

def read_tsv_from_zip(path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(path) as z:
        with z.open(z.namelist()[0]) as f:
            return pd.read_csv(f, sep="\t", dtype=str, low_memory=False)

note_files = sorted(RAW_DIR.glob("notes-*.zip"))
if not note_files:
    raise FileNotFoundError(f"No notes-*.zip files in {RAW_DIR}. Run download_notes.py first.")

notes = pd.concat([read_tsv_from_zip(p) for p in note_files], ignore_index=True)
print(f"Raw notes: {len(notes):,}")

status_path = RAW_DIR / "noteStatusHistory-00000.zip"
if not status_path.exists():
    raise FileNotFoundError(f"Missing {status_path}. Run download_notes.py first.")

status = read_tsv_from_zip(status_path)
status = (
    status[status["currentStatus"] == "CURRENTLY_RATED_HELPFUL"]
    [["noteId", "timestampMillisOfCurrentStatus"]]
    .rename(columns={"timestampMillisOfCurrentStatus": "crh_at_ms"})
)
print(f"CRH rows in status history: {len(status):,}")


# ── Filter 1: 90-day window + CRH join ───────────────────────────────────────

notes["createdAtMillis"] = pd.to_numeric(notes["createdAtMillis"])
notes = notes[
    (notes["createdAtMillis"] >= WINDOW_START_MS) &
    (notes["createdAtMillis"] <  WINDOW_END_MS)
]
print(f"Notes in 90-day window: {len(notes):,}")

cn = notes.merge(status, on="noteId", how="inner")
cn["created_at"] = pd.to_datetime(cn["createdAtMillis"], unit="ms", utc=True)
cn["crh_at"]     = pd.to_datetime(pd.to_numeric(cn["crh_at_ms"]), unit="ms", utc=True)
print(f"CRH notes in window: {len(cn):,}")


# ── Filter 2: English only ────────────────────────────────────────────────────

def is_latin_script(text: str, threshold: float = 0.05) -> bool:
    letters = [c for c in text if unicodedata.category(c).startswith("L")]
    if not letters:
        return True
    non_latin = sum(1 for c in letters if ord(c) > 0x024F)
    return (non_latin / len(letters)) <= threshold

def is_english(text) -> bool:
    if not isinstance(text, str) or len(text.strip()) < 20:
        return True
    if not is_latin_script(text):
        return False
    try:
        return detect(text) == "en"
    except LangDetectException:
        return True

before = len(cn)
cn = cn[cn["summary"].apply(is_english)].copy()
print(f"English filter: {before:,} → {len(cn):,}  (removed {before - len(cn):,})")


# ── Filter 3: low-portability notes ──────────────────────────────────────────
# Remove notes that reference X/Twitter links or describe visual content
# (images, video) — these don't transfer meaningfully to Bluesky posts

PLATFORM_URL_RE = re.compile(
    r'https?://(?:www\.)?(?:twitter\.com|x\.com|instagram\.com)/\S+', re.I
)
VISUAL_RE = re.compile(
    r'\b(?:the|this)\s+(?:photo|image|picture|video|videos|clip|screenshot|thumbnail|footage)\b'
    r'|\b(?:shown|depicted|visible)\s+(?:in|here|above|below)\b'
    r'|\bthe\s+(?:post|tweet)\s+(?:shows|depicts|contains|includes)\b',
    re.I,
)

def is_low_portability(text) -> bool:
    if not isinstance(text, str):
        return False
    return bool(PLATFORM_URL_RE.search(text)) or bool(VISUAL_RE.search(text))

low_port = cn["summary"].apply(is_low_portability)
print(f"Low-portability removed: {low_port.sum():,}")
cn = cn[~low_port].copy()
print(f"After low-portability filter: {len(cn):,}")


# ── Save ──────────────────────────────────────────────────────────────────────

cn["summary"] = cn["summary"].apply(lambda t: html.unescape(t) if isinstance(t, str) else t)

output_cols = ["noteId", "summary", "created_at", "crh_at"]
out_path = CLEANED_DIR / "cn_crh_notes.tsv"
cn[output_cols].to_csv(out_path, sep="\t", index=False)

print(f"\nSaved {len(cn):,} notes → {out_path}")
print(f"File size: {out_path.stat().st_size / 1e6:.1f} MB")
print(f"\nUpload to Fly with:")
print(f"  fly sftp shell")
print(f"  put data/cleaned/cn_crh_notes.tsv /data/cn_crh_notes.tsv")

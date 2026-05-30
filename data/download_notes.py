"""
Downloads the latest Community Notes public data from X (Twitter).

Files downloaded to data/raw/:
  notes-00000.zip, notes-00001.zip, ...     (all note shards)
  noteStatusHistory-00000.zip               (CRH status history)

Usage:
  pip install requests tqdm
  python data/download_notes.py
"""

import sys
from datetime import date, timedelta
from pathlib import Path

import requests
from tqdm import tqdm

RAW_DIR = Path(__file__).parent / "raw"
RAW_DIR.mkdir(exist_ok=True)

BASE_URL = "https://ton.twimg.com/birdwatch-public-data"

# Community Notes data has a ~48h release lag, so we start checking 2 days ago
RELEASE_LAG_DAYS = 2


def find_latest_date() -> date:
    """Walk backwards from (today - lag) until we find a date with data."""
    session = requests.Session()
    for days_back in range(RELEASE_LAG_DAYS, RELEASE_LAG_DAYS + 10):
        d = date.today() - timedelta(days=days_back)
        url = f"{BASE_URL}/{d.year}/{d.month:02d}/{d.day:02d}/notes/notes-00000.zip"
        try:
            r = session.head(url, timeout=10)
            if r.status_code == 200:
                print(f"Latest available data: {d}")
                return d
        except requests.RequestException:
            continue
    raise RuntimeError("Could not find Community Notes data for the past 10 days.")


def download_file(url: str, dest: Path) -> bool:
    """Download a single file with a progress bar. Returns False on 404."""
    if dest.exists():
        print(f"  Already downloaded: {dest.name}")
        return True

    r = requests.get(url, stream=True, timeout=120)
    if r.status_code == 404:
        return False
    r.raise_for_status()

    total = int(r.headers.get("content-length", 0))
    with open(dest, "wb") as f, tqdm(
        total=total, unit="B", unit_scale=True, desc=dest.name, leave=False
    ) as bar:
        for chunk in r.iter_content(chunk_size=65_536):
            f.write(chunk)
            bar.update(len(chunk))

    print(f"  Downloaded: {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)")
    return True


def download_all(d: date) -> None:
    date_path = f"{d.year}/{d.month:02d}/{d.day:02d}"
    print(f"\nDownloading Community Notes for {d} → {RAW_DIR}\n")

    # Download all note shards (stops at first 404)
    print("Notes:")
    for i in range(20):
        fname = f"notes-{i:05d}.zip"
        url = f"{BASE_URL}/{date_path}/notes/{fname}"
        if not download_file(url, RAW_DIR / fname):
            print(f"  (no more shards after index {i - 1})")
            break

    # Download note status history
    print("\nNote status history:")
    fname = "noteStatusHistory-00000.zip"
    url = f"{BASE_URL}/{date_path}/noteStatusHistory/{fname}"
    if not download_file(url, RAW_DIR / fname):
        print(f"  WARNING: {fname} not found at {url}")

    print(f"\nAll done. Files are in {RAW_DIR}")


if __name__ == "__main__":
    try:
        d = find_latest_date()
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    download_all(d)

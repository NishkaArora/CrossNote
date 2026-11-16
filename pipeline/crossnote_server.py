"""HTTP wrapper around crossnote_match.Matcher, served on a unix socket for
the Node labeller (see start.sh and src/crossnote-pipeline.ts).
"""
import json
import math
import re
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from functools import lru_cache

import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from pipeline.crossnote_match import LIBRARY_ROOT, Matcher

matcher: Matcher | None = None


def _log_stats_loop():
    while True:
        time.sleep(60)
        if matcher and matcher.stats["received"]:
            s = matcher.stats
            print(f"[matcher] received={s['received']} length_dropped={s['length_dropped']} "
                  f"tfidf={s['passed_tfidf']} embedding={s['passed_embedding']} "
                  f"stage3_calls={s['stage3_calls']} stage3_errors={s['stage3_errors']} "
                  f"matched={s['matched']} errors={s['errors']}", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global matcher
    matcher = Matcher()
    threading.Thread(target=_log_stats_loop, daemon=True).start()
    yield


app = FastAPI(lifespan=lifespan)


class MatchRequest(BaseModel):
    text: str
    uri: str | None = None


@app.post("/match")
def match(req: MatchRequest) -> dict:
    try:
        return matcher.match(req.text, req.uri)
    except Exception as e:
        matcher.stats["errors"] += 1
        print(f"[matcher] error on {req.uri}: {type(e).__name__}: {e}", flush=True)
        return {"stage": "error", "match": None, "error": f"{type(e).__name__}: {e}"[:300]}


@app.get("/health")
def health() -> dict:
    lib = matcher.library if matcher else None
    return {"ready": bool(matcher and matcher.is_ready()),
            "library": lib.name if lib else None,
            "notes": len(lib.notes) if lib else 0,
            "error": matcher.library_error if matcher else "starting"}


@app.get("/stats")
def stats() -> dict:
    return matcher.stats if matcher else {}


# ── Demo page (src/demo-api.ts) ──────────────────────────────────────────────
# Note text and timestamps for notes in a library snapshot, so the demo page
# can show the note a post was matched to and how long X took to rate it.
# Never returns X post text (X Developer Agreement): the page embeds the X post.

_SNAPSHOT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_NOTE_COLUMNS = ["noteId", "tweetId", "summary", "createdAtMillis", "firstHelpfulMillis", "x_post_created_at"]


def _iso_from_millis(ms):
    if ms is None or (isinstance(ms, float) and math.isnan(ms)):
        return None
    return datetime.fromtimestamp(float(ms) / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


@lru_cache(maxsize=8)
def _snapshot_notes(name: str):
    path = LIBRARY_ROOT / name
    manifest = json.loads((path / "manifest.json").read_text())
    notes = pd.read_parquet(path / "notes.parquet", columns=_NOTE_COLUMNS)
    return manifest, notes.set_index("noteId", drop=False)


class DemoNotesRequest(BaseModel):
    library: str
    note_ids: list[str] = Field(max_length=200)


@app.post("/demo/notes")
def demo_notes(req: DemoNotesRequest) -> dict:
    if not _SNAPSHOT_NAME.match(req.library) or ".." in req.library:
        raise HTTPException(400, "bad library name")
    try:
        manifest, notes = _snapshot_notes(req.library)
    except FileNotFoundError:
        raise HTTPException(404, "library snapshot not found")
    out = {}
    for note_id in dict.fromkeys(req.note_ids):
        if note_id not in notes.index:
            continue
        row = notes.loc[note_id]
        x_created = row["x_post_created_at"]
        out[note_id] = {
            "note_id": note_id,
            "tweet_id": str(row["tweetId"]),
            "text": row["summary"],
            "note_created_at": _iso_from_millis(row["createdAtMillis"]),
            "note_helpful_at": _iso_from_millis(row["firstHelpfulMillis"]),
            "x_post_created_at": x_created if isinstance(x_created, str) and x_created else None,
        }
    s1 = matcher.cfg["stage1"] if matcher else {}
    return {
        "library": {"name": req.library, "export_date": manifest.get("export_date"),
                    "built_at": manifest.get("built_at"), "notes": int(len(notes))},
        "thresholds": {"min_words_exclusive": s1.get("min_words_exclusive"),
                       "tfidf": s1.get("tfidf_threshold"), "embedding": s1.get("embedding_threshold"),
                       "top_k": matcher.cfg["stage2"]["top_k"] if matcher else None},
        "notes": out,
    }


class DemoRerunRequest(BaseModel):
    text: str = Field(min_length=1, max_length=3000)
    library: str | None = None


@app.post("/demo/rerun")
def demo_rerun(req: DemoRerunRequest) -> dict:
    """What-if run for the demo page: the edited post through Stages 1-3 with
    nothing counted, logged or labelled (see Matcher.rerun)."""
    try:
        result = matcher.rerun(req.text, req.library)
    except Exception as e:
        print(f"[matcher] demo rerun error: {type(e).__name__}: {e}", flush=True)
        raise HTTPException(502, "rerun failed")
    s1 = matcher.cfg["stage1"]
    result["thresholds"] = {"min_words_exclusive": s1["min_words_exclusive"], "tfidf": s1["tfidf_threshold"],
                            "embedding": s1["embedding_threshold"], "top_k": matcher.cfg["stage2"]["top_k"]}
    return result

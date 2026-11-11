"""HTTP wrapper around crossnote_match.Matcher, served on a unix socket for
the Node labeller (see start.sh and src/crossnote-pipeline.ts).

Replaces pipeline/server.py (BM25 -> cross-encoder -> gpt-4o-mini).
"""
import threading
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from pipeline.crossnote_match import Matcher

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

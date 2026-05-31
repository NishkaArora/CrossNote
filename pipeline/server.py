import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from pipeline import retrieval_reranking as pipeline


def _log_stats_loop():
    import time
    while True:
        time.sleep(60)
        s = pipeline.stats
        if s["received"] > 0:
            print(
                f"[pipeline] received={s['received']} "
                f"bm25={s['passed_bm25']} ({100*s['passed_bm25']/s['received']:.1f}%) "
                f"ce={s['passed_ce']} "
                f"llm={s['passed_llm']}"
            )


@asynccontextmanager
async def lifespan(app: FastAPI):
    pipeline.load()
    threading.Thread(target=_log_stats_loop, daemon=True).start()
    yield


app = FastAPI(lifespan=lifespan)


class AnalyzeRequest(BaseModel):
    text: str


class AnalyzeResponse(BaseModel):
    label: str | None
    note:  str | None
    score: float | None


@app.post("/analyze", response_model=AnalyzeResponse)
def analyze(req: AnalyzeRequest) -> AnalyzeResponse:
    result = pipeline.analyze(req.text)
    if result is None:
        return AnalyzeResponse(label=None, note=None, score=None)
    return AnalyzeResponse(label="misinformation", note=result["note"], score=result["score"])


@app.get("/health")
def health() -> dict:
    return {"ready": pipeline.is_ready(), "llm": pipeline._openai is not None}


@app.get("/stats")
def stats() -> dict:
    return pipeline.stats


@app.post("/debug")
def debug(req: AnalyzeRequest) -> dict:
    return pipeline.debug_analyze(req.text)

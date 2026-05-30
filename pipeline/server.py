from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from pipeline import retrieval_reranking as pipeline


@asynccontextmanager
async def lifespan(app: FastAPI):
    pipeline.load()
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
    return {"ready": pipeline.is_ready()}

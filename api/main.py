from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI
from pydantic import BaseModel

from rag.pipeline import RAGPipeline


CONFIG_PATH = "configs/corpora/financial_reports.yaml"

app = FastAPI(
    title="Financial Agentic RAG API",
    description="API demo for SEC 10-K financial document intelligence system",
    version="0.1.0",
)

baseline_pipeline = RAGPipeline(CONFIG_PATH)


class QueryRequest(BaseModel):
    query: str
    mode: Literal["baseline", "agentic"] = "baseline"


@app.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "system": "financial-rag",
    }


@app.post("/query")
def query(request: QueryRequest) -> dict[str, Any]:
    start = time.perf_counter()

    if request.mode == "baseline":
        result = baseline_pipeline.run(request.query)
        mode_used = "baseline"

    latency_seconds = round(time.perf_counter() - start, 3)

    return {
        "query": result.query,
        "mode": mode_used,
        "answer": result.answer,
        "citations": [
            {
                "citation_id": citation.citation_id,
                "source_path": citation.source_path,
                "section_title": citation.section_title,
                "section_path": citation.section_path,
                "retrieval_score": citation.retrieval_score,
                "rerank_score": citation.rerank_score,
            }
            for citation in result.citations
        ],
        "metadata": result.metadata,
        "latency_seconds": latency_seconds,
    }


@app.get("/evaluation/summary")
def evaluation_summary() -> dict[str, Any]:
    summary_path = Path(
        "data/processed/financial_reports/evaluation/baseline_v1_summary.json"
    )

    if not summary_path.exists():
        return {
            "status": "missing",
            "message": "Baseline evaluation summary not found.",
        }

    import json

    return json.loads(summary_path.read_text(encoding="utf-8"))
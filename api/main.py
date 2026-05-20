from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI
from pydantic import BaseModel

from agentic.pipeline import AgenticRAGPipeline
from rag.pipeline import RAGPipeline


CONFIG_PATH = "configs/corpora/financial_reports.yaml"

app = FastAPI(
    title="Financial Agentic RAG API",
    description="API demo for SEC 10-K financial document intelligence system",
    version="0.2.0",
)

baseline_pipeline = RAGPipeline(CONFIG_PATH)
agentic_pipeline = AgenticRAGPipeline(CONFIG_PATH)


class QueryRequest(BaseModel):
    query: str
    mode: Literal["baseline", "agentic"] = "agentic"


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
    else:
        result = agentic_pipeline.run(request.query)

    latency_seconds = round(time.perf_counter() - start, 3)

    metadata = result.metadata or {}

    return {
        "query": result.query,
        "mode": request.mode,
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
        "query_plan": metadata.get("query_plan"),
        "metadata_filters": metadata.get("metadata_filters"),
        "evidence_check": metadata.get("evidence_check"),
        "pipeline_metadata": {
            "collection_name": metadata.get("collection_name"),
            "embedding_model": metadata.get("embedding_model"),
            "reranker_model": metadata.get("reranker_model"),
            "retrieval_top_k": metadata.get("retrieval_top_k"),
            "final_top_k": metadata.get("final_top_k"),
            "max_context_chunks": metadata.get("max_context_chunks"),
            "answer_generator": metadata.get("answer_generator"),
        },
        "latency_seconds": latency_seconds,
    }


@app.get("/evaluation/summary")
def evaluation_summary() -> dict[str, Any]:
    baseline_path = Path(
        "data/processed/financial_reports/evaluation/baseline_v1_summary.json"
    )

    agentic_path = Path(
        "data/processed/financial_reports/evaluation/agentic_v1_summary.json"
    )

    output: dict[str, Any] = {}

    if baseline_path.exists():
        output["baseline"] = json.loads(
            baseline_path.read_text(encoding="utf-8")
        )

    if agentic_path.exists():
        output["agentic"] = json.loads(
            agentic_path.read_text(encoding="utf-8")
        )

    if not output:
        return {
            "status": "missing",
            "message": "No evaluation summaries found.",
        }

    return output
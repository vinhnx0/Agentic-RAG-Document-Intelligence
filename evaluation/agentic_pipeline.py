# evaluation/agentic_pipeline.py

from __future__ import annotations

import json
import time
from pathlib import Path
from statistics import mean
from typing import Any

import yaml

from agentic.pipeline import AgenticRAGPipeline
from evaluation.metrics import citation_coverage, hit_at_k, reciprocal_rank
from utils.io import to_jsonable


class AgenticEvaluationPipeline:
    def __init__(self, config_path: str | Path) -> None:
        self.config_path = Path(config_path)
        self.config = self._load_config()

        self.evaluation_config = self.config.get("evaluation", {})
        self.reranking_config = self.config.get("reranking", {})
        self.retrieval_config = self.config.get("retrieval", {})

        self.agentic_rag_pipeline = AgenticRAGPipeline(config_path)

    def _load_config(self) -> dict[str, Any]:
        with self.config_path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _load_eval_queries(self) -> list[dict[str, Any]]:
        testset_path = self.evaluation_config.get("testset_path")
        if not testset_path:
            raise ValueError("Missing evaluation.testset_path in config")

        path = Path(testset_path)
        if not path.exists():
            raise FileNotFoundError(f"Evaluation testset not found: {path}")

        return json.loads(path.read_text(encoding="utf-8"))

    def run(self) -> dict[str, Any]:
        eval_queries = self._load_eval_queries()

        retrieval_top_k = self.retrieval_config.get("top_k", 20)
        final_top_k = self.reranking_config.get("final_top_k", 5)

        query_reports: list[dict[str, Any]] = []

        for item in eval_queries:
            query_id = item["query_id"]
            query = item["query"]

            start_time = time.perf_counter()

            rag_answer = self.agentic_rag_pipeline.run(
                query=query,
                retrieval_top_k=retrieval_top_k,
                final_top_k=final_top_k,
            )

            latency_seconds = time.perf_counter() - start_time
            rag_output = to_jsonable(rag_answer)

            context_chunks = rag_output["context_chunks"]
            evidence_check = rag_output["metadata"].get("evidence_check", {})
            query_plan = rag_output["metadata"].get("query_plan", {})
            metadata_filters = rag_output["metadata"].get("metadata_filters", {})

            expected_behavior = item.get("expected_behavior", "answer")
            predicted_behavior = (
                "answer"
                if evidence_check.get("sufficient")
                else "insufficient_evidence"
            )

            query_report = {
                "query_id": query_id,
                "query": query,
                "expected_behavior": expected_behavior,
                "predicted_behavior": predicted_behavior,
                "behavior_correct": expected_behavior == predicted_behavior,
                "latency_seconds": latency_seconds,

                "query_plan": query_plan,
                "metadata_filters": metadata_filters,
                "evidence_check": evidence_check,

                "expected_company": item.get("expected_company"),
                "expected_years": item.get("expected_years", []),
                "expected_section_types": item.get("expected_section_types", []),
                "expected_form_items": item.get("expected_form_items", []),

                "retrieval": {
                    "hit_at_1": hit_at_k(context_chunks, item, 1),
                    "hit_at_5": hit_at_k(context_chunks, item, 5),
                    "hit_at_10": hit_at_k(context_chunks, item, 10),
                    "mrr": reciprocal_rank(context_chunks, item),
                },
                "rag": {
                    "citation_count": len(rag_output["citations"]),
                    "context_chunk_count": len(context_chunks),
                    "citation_coverage": citation_coverage(rag_output),
                    "answer_preview": rag_output["answer"][:500],
                },
                "top_context_preview": [
                    self._preview_result(result)
                    for result in context_chunks[:5]
                ],
            }

            query_reports.append(query_report)

        return self._build_report(query_reports)

    @staticmethod
    def _preview_result(result: dict[str, Any]) -> dict[str, Any]:
        metadata = result.get("metadata", {}) or {}

        return {
            "rerank_rank": result.get("rerank_rank"),
            "rerank_score": result.get("rerank_score"),
            "retrieval_rank": result.get("retrieval_rank"),
            "section_title": result.get("section_title"),
            "source_path": result.get("source_path"),
            "company": metadata.get("company"),
            "fiscal_year": metadata.get("fiscal_year"),
            "form_item": metadata.get("form_item"),
            "section_type": metadata.get("section_type"),
        }

    def _build_report(self, query_reports: list[dict[str, Any]]) -> dict[str, Any]:
        if not query_reports:
            raise ValueError("No query reports generated")

        insufficient_items = [
            report for report in query_reports
            if report["expected_behavior"] == "insufficient_evidence"
        ]

        return {
            "query_count": len(query_reports),
            "retrieval_summary": {
                "hit_at_1": mean(report["retrieval"]["hit_at_1"] for report in query_reports),
                "hit_at_5": mean(report["retrieval"]["hit_at_5"] for report in query_reports),
                "hit_at_10": mean(report["retrieval"]["hit_at_10"] for report in query_reports),
                "mrr": mean(report["retrieval"]["mrr"] for report in query_reports),
            },
            "rag_summary": {
                "avg_citation_coverage": mean(report["rag"]["citation_coverage"] for report in query_reports),
                "avg_citation_count": mean(report["rag"]["citation_count"] for report in query_reports),
            },
            "agentic_summary": {
                "behavior_accuracy": mean(report["behavior_correct"] for report in query_reports),
                "insufficient_evidence_accuracy": (
                    mean(report["behavior_correct"] for report in insufficient_items)
                    if insufficient_items
                    else None
                ),
                "average_latency_seconds": mean(report["latency_seconds"] for report in query_reports),
            },
            "query_reports": query_reports,
        }

    def get_processed_data_dir(self) -> Path:
        return Path(self.config["processed_data_dir"])

    def get_output_stage_name(self) -> str:
        return self.evaluation_config.get("output_stage_name", "evaluation")
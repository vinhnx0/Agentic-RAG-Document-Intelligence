# agentic/pipeline.py

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from agentic.evidence_checker import EvidenceSufficiencyChecker
from agentic.metadata_filter import MetadataFilterBuilder
from agentic.query_planner import RuleBasedQueryPlanner
from rag.citations import CitationBuilder
from rag.generators import AnswerGeneratorRegistry
from reranking.pipeline import RerankingPipeline
from schemas.documents import RAGAnswer


class AgenticRAGPipeline:
    """
    Agentic metadata-aware RAG pipeline.

    Flow:
    query
    -> query planner
    -> metadata filter builder
    -> filtered retrieval
    -> reranking
    -> evidence checker
    -> answer with citations or insufficient evidence response
    """

    def __init__(self, config_path: str | Path) -> None:
        self.config_path = Path(config_path)
        self.config = self._load_config()

        self.rag_config = self.config.get("rag", {})
        self.reranking_config = self.config.get("reranking", {})
        self.retrieval_config = self.config.get("retrieval", {})

        self.query_planner = RuleBasedQueryPlanner()
        self.filter_builder = MetadataFilterBuilder()
        self.evidence_checker = EvidenceSufficiencyChecker()

        self.reranking_pipeline = RerankingPipeline(config_path)
        self.citation_builder = CitationBuilder()
        self.generator_registry = self._build_generator_registry()

    def _load_config(self) -> dict[str, Any]:
        with self.config_path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _build_generator_registry(self) -> AnswerGeneratorRegistry:
        return AnswerGeneratorRegistry(
            generator_type=self.rag_config.get("answer_generator", "extractive"),
            max_chars_per_chunk=self.rag_config.get(
                "max_context_chars_per_chunk",
                1200,
            ),
        )

    def run(
        self,
        query: str,
        retrieval_top_k: int | None = None,
        final_top_k: int | None = None,
    ) -> RAGAnswer:
        clean_query = query.strip()

        if not clean_query:
            raise ValueError("query must not be empty")

        max_context_chunks = self.rag_config.get("max_context_chunks", 5)

        resolved_retrieval_top_k = (
            retrieval_top_k
            or self.retrieval_config.get("top_k", 20)
        )

        resolved_final_top_k = (
            final_top_k
            or self.reranking_config.get("final_top_k", 5)
        )

        query_plan = self.query_planner.plan(clean_query)
        metadata_filters = self.filter_builder.build(query_plan)

        retrieval_output = self.reranking_pipeline.retrieval_pipeline.run(
            query=clean_query,
            top_k=resolved_retrieval_top_k,
            metadata_filters=metadata_filters,
        )

        reranker = self.reranking_pipeline.reranker_registry.get_reranker()
        reranked_results = reranker.rerank(
            query=clean_query,
            results=retrieval_output["results"],
            final_top_k=resolved_final_top_k,
        )

        context_chunks = reranked_results[:max_context_chunks]

        evidence_check = self.evidence_checker.check(
            query_plan=query_plan,
            chunks=context_chunks,
        )

        if not evidence_check.sufficient:
            return RAGAnswer(
                query=clean_query,
                answer=(
                    "I could not find sufficient evidence to answer this question. "
                    f"Reason: {evidence_check.reason}"
                ),
                citations=[],
                context_chunks=context_chunks,
                metadata={
                    "mode": "agentic",
                    "query_plan": query_plan.to_dict(),
                    "metadata_filters": metadata_filters,
                    "evidence_check": evidence_check.to_dict(),
                    "collection_name": retrieval_output["collection_name"],
                    "embedding_model": retrieval_output["embedding_model"],
                    "reranker_model": reranker.get_model_name(),
                    "retrieval_top_k": resolved_retrieval_top_k,
                    "final_top_k": resolved_final_top_k,
                    "max_context_chunks": max_context_chunks,
                },
            )

        citations = self.citation_builder.build(context_chunks)

        generator = self.generator_registry.get_generator()
        answer = generator.generate(
            query=clean_query,
            context_chunks=context_chunks,
        )

        return RAGAnswer(
            query=clean_query,
            answer=answer,
            citations=citations,
            context_chunks=context_chunks,
            metadata={
                "mode": "agentic",
                "query_plan": query_plan.to_dict(),
                "metadata_filters": metadata_filters,
                "evidence_check": evidence_check.to_dict(),
                "collection_name": retrieval_output["collection_name"],
                "embedding_model": retrieval_output["embedding_model"],
                "reranker_model": reranker.get_model_name(),
                "retrieval_top_k": resolved_retrieval_top_k,
                "final_top_k": resolved_final_top_k,
                "max_context_chunks": max_context_chunks,
                "answer_generator": self.rag_config.get(
                    "answer_generator",
                    "extractive",
                ),
            },
        )

    def get_processed_data_dir(self) -> Path:
        processed_data_dir = self.config.get("processed_data_dir")
        if not processed_data_dir:
            raise KeyError("Missing processed_data_dir in config")
        return Path(processed_data_dir)
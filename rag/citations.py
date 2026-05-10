# rag/citations.py

from __future__ import annotations

from typing import Any

from schemas.documents import Citation


class CitationBuilder:
    def build(self, results: list[dict[str, Any]]) -> list[Citation]:
        citations: list[Citation] = []

        for citation_id, result in enumerate(results, start=1):
            citation = Citation(
                citation_id=citation_id,
                chunk_id=result.get("chunk_id"),
                doc_id=result.get("doc_id"),
                source_path=result.get("source_path"),
                section_title=result.get("section_title"),
                section_path=result.get("section_path", []),
                retrieval_score=result.get("retrieval_score"),
                rerank_score=result.get("rerank_score"),
            )
            citations.append(citation)

        return citations

    def format_citation_marker(self, citation_id: int) -> str:
        return f"[{citation_id}]"
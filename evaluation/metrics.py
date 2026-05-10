# evaluation/metrics.py

from __future__ import annotations

from typing import Any


def contains_expected_source(
    result: dict[str, Any],
    expected_source_contains: str | None,
) -> bool:
    if not expected_source_contains:
        return False

    source_path = result.get("source_path") or ""
    return expected_source_contains.lower() in source_path.lower()


def contains_section_keyword(
    result: dict[str, Any],
    expected_section_keywords: list[str],
) -> bool:
    if not expected_section_keywords:
        return False

    section_title = result.get("section_title") or ""
    section_path = " ".join(result.get("section_path") or [])

    haystack = f"{section_title} {section_path}".lower()

    return any(
        keyword.lower() in haystack
        for keyword in expected_section_keywords
    )


def hit_at_k(
    results: list[dict[str, Any]],
    expected_source_contains: str | None,
    expected_section_keywords: list[str],
    k: int,
) -> bool:
    top_results = results[:k]

    for result in top_results:
        source_match = contains_expected_source(
            result=result,
            expected_source_contains=expected_source_contains,
        )
        section_match = contains_section_keyword(
            result=result,
            expected_section_keywords=expected_section_keywords,
        )

        if source_match and section_match:
            return True

    return False


def reciprocal_rank(
    results: list[dict[str, Any]],
    expected_source_contains: str | None,
    expected_section_keywords: list[str],
) -> float:
    for index, result in enumerate(results, start=1):
        source_match = contains_expected_source(
            result=result,
            expected_source_contains=expected_source_contains,
        )
        section_match = contains_section_keyword(
            result=result,
            expected_section_keywords=expected_section_keywords,
        )

        if source_match and section_match:
            return 1.0 / index

    return 0.0


def citation_coverage(rag_output: dict[str, Any]) -> float:
    context_chunks = rag_output.get("context_chunks", [])
    citations = rag_output.get("citations", [])

    if not context_chunks:
        return 0.0

    return len(citations) / len(context_chunks)
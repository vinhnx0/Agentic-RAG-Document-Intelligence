# evaluation/metrics.py

from __future__ import annotations

from typing import Any


def _normalize(value: Any) -> str:
    return str(value or "").strip().lower()


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def financial_metadata_match(
    result: dict[str, Any],
    expected_company: str | None = None,
    expected_years: list[int] | None = None,
    expected_section_types: list[str] | None = None,
    expected_form_items: list[str] | None = None,
) -> bool:
    metadata = result.get("metadata", {}) or {}

    company_match = (
        not expected_company
        or _normalize(metadata.get("company")) == _normalize(expected_company)
    )

    expected_years = expected_years or []
    year_match = (
        not expected_years
        or metadata.get("fiscal_year") in expected_years
    )

    expected_section_types = expected_section_types or []
    section_type_match = (
        not expected_section_types
        or _normalize(metadata.get("section_type"))
        in {_normalize(item) for item in expected_section_types}
    )

    expected_form_items = expected_form_items or []
    form_item_match = (
        not expected_form_items
        or _normalize(metadata.get("form_item"))
        in {_normalize(item) for item in expected_form_items}
    )

    return company_match and year_match and section_type_match and form_item_match


def contains_expected_source(
    result: dict[str, Any],
    expected_source_contains: str | None,
) -> bool:
    if not expected_source_contains:
        return True

    source_path = result.get("source_path") or ""
    return expected_source_contains.lower() in source_path.lower()


def contains_section_keyword(
    result: dict[str, Any],
    expected_section_keywords: list[str],
) -> bool:
    if not expected_section_keywords:
        return True

    section_title = result.get("section_title") or ""
    section_path = " ".join(result.get("section_path") or [])

    haystack = f"{section_title} {section_path}".lower()

    return any(
        keyword.lower() in haystack
        for keyword in expected_section_keywords
    )


def result_matches_expected(
    result: dict[str, Any],
    item: dict[str, Any],
) -> bool:
    """
    Dataset B preferred scoring:
    - use metadata fields if available in eval item

    Dataset A fallback scoring:
    - use source path + section keyword matching
    """

    has_financial_expectations = any(
        key in item
        for key in [
            "expected_company",
            "expected_years",
            "expected_section_types",
            "expected_form_items",
        ]
    )

    if has_financial_expectations:
        return financial_metadata_match(
            result=result,
            expected_company=item.get("expected_company"),
            expected_years=_as_list(item.get("expected_years")),
            expected_section_types=_as_list(item.get("expected_section_types")),
            expected_form_items=_as_list(item.get("expected_form_items")),
        )

    return (
        contains_expected_source(
            result=result,
            expected_source_contains=item.get("expected_source_contains"),
        )
        and contains_section_keyword(
            result=result,
            expected_section_keywords=item.get("expected_section_keywords", []),
        )
    )


def hit_at_k(
    results: list[dict[str, Any]],
    item: dict[str, Any],
    k: int,
) -> bool:
    top_results = results[:k]

    for result in top_results:
        if result_matches_expected(result=result, item=item):
            return True

    return False


def reciprocal_rank(
    results: list[dict[str, Any]],
    item: dict[str, Any],
) -> float:
    for index, result in enumerate(results, start=1):
        if result_matches_expected(result=result, item=item):
            return 1.0 / index

    return 0.0


def citation_coverage(rag_output: dict[str, Any]) -> float:
    context_chunks = rag_output.get("context_chunks", [])
    citations = rag_output.get("citations", [])

    if not context_chunks:
        return 0.0

    return len(citations) / len(context_chunks)
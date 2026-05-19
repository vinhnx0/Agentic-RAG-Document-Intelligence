# agentic/evidence_checker.py

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any

from agentic.query_planner import QueryPlan


METRIC_TERMS = {
    "net_sales": ["net sales", "total net sales", "revenue"],
    "operating_income": ["operating income"],
    "net_income": ["net income"],
    "cash_flow": ["cash flow", "operating cash flow"],
}


@dataclass(slots=True)
class EvidenceCheckResult:
    sufficient: bool
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class EvidenceSufficiencyChecker:
    """
    Simple rule-based checker for Phase 12.

    Purpose:
    - Prevent unsupported answers.
    - Check whether retrieved evidence matches the query plan.
    - Keep it explainable and easy to debug.
    """

    def __init__(self, min_rerank_score: float | None = None) -> None:
        self.min_rerank_score = min_rerank_score

    def check(
        self,
        query_plan: QueryPlan,
        chunks: list[dict[str, Any]],
    ) -> EvidenceCheckResult:
        if not chunks:
            return EvidenceCheckResult(
                sufficient=False,
                reason="No evidence chunks were retrieved.",
            )
        
        if query_plan.expected_behavior == "insufficient_evidence":
            return EvidenceCheckResult(
                sufficient=False,
                reason="Query asks for opinion, prediction, or unsupported information.",
            )

        if self.min_rerank_score is not None:
            best_score = chunks[0].get("rerank_score")
            if best_score is not None and best_score < self.min_rerank_score:
                return EvidenceCheckResult(
                    sufficient=False,
                    reason=f"Best rerank score is too low: {best_score:.4f}.",
                )

        if query_plan.company and not self._has_company(query_plan.company, chunks):
            return EvidenceCheckResult(
                sufficient=False,
                reason=f"Missing evidence for company: {query_plan.company}.",
            )

        if query_plan.years and not self._has_all_years(query_plan.years, chunks):
            return EvidenceCheckResult(
                sufficient=False,
                reason=f"Missing evidence for requested years: {query_plan.years}.",
            )

        if query_plan.section_types and not self._has_section_type(
            query_plan.section_types,
            chunks,
        ):
            return EvidenceCheckResult(
                sufficient=False,
                reason=f"Missing evidence for section types: {query_plan.section_types}.",
            )

        if query_plan.requires_comparison and len(query_plan.years) > 1:
            if not self._has_all_years(query_plan.years, chunks):
                return EvidenceCheckResult(
                    sufficient=False,
                    reason="Comparison query is missing evidence for one or more requested years.",
                )

        if query_plan.metric and not self._has_metric_terms(query_plan.metric, chunks):
            return EvidenceCheckResult(
                sufficient=False,
                reason=f"Missing evidence for metric: {query_plan.metric}.",
            )

        return EvidenceCheckResult(
            sufficient=True,
            reason="Evidence matches requested company, year, section, and metric requirements.",
        )

    @staticmethod
    def _has_company(
        expected_company: str,
        chunks: list[dict[str, Any]],
    ) -> bool:
        expected = expected_company.strip().lower()

        return any(
            ((chunk.get("metadata", {}) or {}).get("company") or "").strip().lower()
            == expected
            for chunk in chunks
        )

    @staticmethod
    def _has_all_years(
        expected_years: list[int],
        chunks: list[dict[str, Any]],
    ) -> bool:
        found_years = {
            (chunk.get("metadata", {}) or {}).get("fiscal_year")
            for chunk in chunks
        }

        return all(year in found_years for year in expected_years)

    @staticmethod
    def _has_section_type(
        expected_section_types: list[str],
        chunks: list[dict[str, Any]],
    ) -> bool:
        expected = {item.strip().lower() for item in expected_section_types}

        found = {
            ((chunk.get("metadata", {}) or {}).get("section_type") or "")
            .strip()
            .lower()
            for chunk in chunks
        }

        return bool(expected.intersection(found))

    @staticmethod
    def _has_metric_terms(
        metric: str,
        chunks: list[dict[str, Any]],
    ) -> bool:
        terms = METRIC_TERMS.get(metric, [metric.replace("_", " ")])

        evidence_text = " ".join(
            [
                chunk.get("text") or ""
                for chunk in chunks
            ]
        ).lower()

        return any(term.lower() in evidence_text for term in terms)
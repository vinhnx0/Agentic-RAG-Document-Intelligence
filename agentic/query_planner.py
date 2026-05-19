# agentic/query_planner.py

from __future__ import annotations

import re
from dataclasses import dataclass, field, asdict
from typing import Any


YEAR_PATTERN = re.compile(r"\b(20\d{2})\b")


COMPANY_ALIASES = {
    "apple": ("Apple", "AAPL"),
    "aapl": ("Apple", "AAPL"),
}


METRIC_KEYWORDS = {
    "net_sales": ["net sales", "revenue", "sales"],
    "operating_income": ["operating income"],
    "net_income": ["net income", "profit"],
    "cash_flow": ["cash flow", "operating cash flow"],
}


SECTION_KEYWORDS = {
    "risk_factors": ["risk", "risks", "risk factor", "uncertainty", "challenge"],
    "management_discussion": [
        "management discussion",
        "md&a",
        "discussion and analysis",
        "performance",
        "results of operations",
    ],
    "financial_statements": [
        "net sales",
        "revenue",
        "sales",
        "net income",
        "operating income",
        "balance sheet",
        "cash flow",
        "financial statement",
    ],
    "market_risk": ["market risk", "interest rate", "foreign exchange", "currency"],
    "cybersecurity": ["cybersecurity", "cyber security", "security incident"],
}


COMPARISON_KEYWORDS = [
    "compare",
    "change",
    "changed",
    "increase",
    "decrease",
    "trend",
    "from",
    "between",
    "over time",
]


INSUFFICIENT_KEYWORDS = [
    "personally think",
    "opinion",
    "predict",
    "future stock price",
    "should i invest",
    "recommend buying",
]


@dataclass(slots=True)
class QueryPlan:
    query: str
    intent: str
    company: str | None = None
    ticker: str | None = None
    years: list[int] = field(default_factory=list)
    metric: str | None = None
    section_types: list[str] = field(default_factory=list)
    requires_comparison: bool = False
    expected_behavior: str = "answer"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class RuleBasedQueryPlanner:
    """
    Simple Phase 9 query planner.

    Purpose:
    - Extract company/year/metric/section intent from the query.
    - Keep it rule-based for speed and easier debugging.
    - Do not call LLMs.
    - Do not apply metadata filters yet.
    """

    def plan(self, query: str) -> QueryPlan:
        clean_query = query.strip()

        if not clean_query:
            raise ValueError("query must not be empty")

        normalized_query = clean_query.lower()

        company, ticker = self._extract_company(normalized_query)
        years = self._extract_years(normalized_query)
        metric = self._extract_metric(normalized_query)
        section_types = self._extract_section_types(normalized_query)
        requires_comparison = self._requires_comparison(normalized_query, years)
        intent = self._infer_intent(
            metric=metric,
            section_types=section_types,
            requires_comparison=requires_comparison,
        )

        return QueryPlan(
            query=clean_query,
            intent=intent,
            company=company,
            ticker=ticker,
            years=years,
            metric=metric,
            section_types=section_types,
            requires_comparison=requires_comparison,
            expected_behavior = (
                "insufficient_evidence"
                if any(keyword in normalized_query for keyword in INSUFFICIENT_KEYWORDS)
                else "answer"
            )
        )

    @staticmethod
    def _extract_company(normalized_query: str) -> tuple[str | None, str | None]:
        for alias, company_info in COMPANY_ALIASES.items():
            if alias in normalized_query:
                return company_info

        return None, None

    @staticmethod
    def _extract_years(normalized_query: str) -> list[int]:
        years = sorted({
            int(match.group(1))
            for match in YEAR_PATTERN.finditer(normalized_query)
        })
        return years

    @staticmethod
    def _extract_metric(normalized_query: str) -> str | None:
        for metric, keywords in METRIC_KEYWORDS.items():
            if any(keyword in normalized_query for keyword in keywords):
                return metric

        return None

    @staticmethod
    def _extract_section_types(normalized_query: str) -> list[str]:
        # Priority rule: market risk is a specific SEC section, so do not also add general risk_factors
        if "market risk" in normalized_query or "market risks" in normalized_query:
            return ["market_risk"]

        matched_sections: list[str] = []

        for section_type, keywords in SECTION_KEYWORDS.items():
            if any(keyword in normalized_query for keyword in keywords):
                matched_sections.append(section_type)

        return matched_sections

    @staticmethod
    def _requires_comparison(
        normalized_query: str,
        years: list[int],
    ) -> bool:
        has_comparison_word = any(
            keyword in normalized_query
            for keyword in COMPARISON_KEYWORDS
        )

        return has_comparison_word or len(years) > 1

    @staticmethod
    def _infer_intent(
        metric: str | None,
        section_types: list[str],
        requires_comparison: bool,
    ) -> str:
        if requires_comparison:
            return "comparison"

        if metric:
            return "metric_lookup"

        if "risk_factors" in section_types:
            return "risk_lookup"

        if section_types:
            return "section_lookup"

        return "general_lookup"
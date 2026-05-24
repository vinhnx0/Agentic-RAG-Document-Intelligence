# agentic/metadata_filter.py

from __future__ import annotations

from typing import Any

from agentic.query_planner import QueryPlan


class MetadataFilterBuilder:
    """
    Convert QueryPlan into retrieval metadata filters.

    Important:
    - This does NOT execute retrieval.
    - This does NOT build Qdrant Filter objects yet.
    - This only creates normalized filter dictionaries.
    """

    def build(self, query_plan: QueryPlan) -> dict[str, Any]:
        filters: dict[str, Any] = {}

        if query_plan.company:
            filters["company"] = query_plan.company

        if len(query_plan.years) == 1:
            filters["fiscal_year"] = query_plan.years[0]

        if len(query_plan.section_types) == 1:
            filters["section_type"] = query_plan.section_types[0]

        return filters  
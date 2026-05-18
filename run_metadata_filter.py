# run_metadata_filter.py

from agentic.metadata_filter import MetadataFilterBuilder
from agentic.query_planner import RuleBasedQueryPlanner


def main() -> None:
    planner = RuleBasedQueryPlanner()
    builder = MetadataFilterBuilder()

    test_queries = [
        "What was Apple’s net sales in 2023?",
        "What risks did Apple mention in 2024?",
        "What market risks did Apple report in 2023?",
    ]

    for query in test_queries:
        plan = planner.plan(query)
        filters = builder.build(plan)

        print("\nQUERY:")
        print(query)

        print("PLAN:")
        print(plan.to_dict())

        print("FILTERS:")
        print(filters)


if __name__ == "__main__":
    main()
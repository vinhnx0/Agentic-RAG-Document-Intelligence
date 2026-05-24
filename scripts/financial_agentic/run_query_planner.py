# run_query_planner.py

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from agentic.query_planner import RuleBasedQueryPlanner


def main() -> None:
    planner = RuleBasedQueryPlanner()

    test_queries = [
        "What was Apple’s net sales in 2023?",
        "What risks did Apple mention in 2024?",
        "How did Apple’s revenue change from 2022 to 2024?",
        "What did Apple say about cybersecurity in 2025?",
        "What market risks did Apple report in 2023?",
    ]

    for query in test_queries:
        plan = planner.plan(query)
        print("\nQUERY:")
        print(query)
        print("PLAN:")
        print(plan.to_dict())


if __name__ == "__main__":
    main()
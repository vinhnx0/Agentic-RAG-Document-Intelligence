# run_metadata_retrieval.py

from agentic.metadata_filter import MetadataFilterBuilder
from agentic.query_planner import RuleBasedQueryPlanner
from retrieval.pipeline import RetrievalPipeline


CONFIG_PATH = "configs/corpora/financial_reports.yaml"


def main() -> None:
    query = "What market risks did Apple report in 2023?"

    planner = RuleBasedQueryPlanner()
    filter_builder = MetadataFilterBuilder()
    retrieval_pipeline = RetrievalPipeline(CONFIG_PATH)

    query_plan = planner.plan(query)
    metadata_filters = filter_builder.build(query_plan)

    output = retrieval_pipeline.run(
        query=query,
        metadata_filters=metadata_filters,
    )

    print("QUERY:")
    print(query)

    print("\nQUERY PLAN:")
    print(query_plan.to_dict())

    print("\nMETADATA FILTERS:")
    print(metadata_filters)

    print("\nRESULT COUNT:")
    print(output["result_count"])

    print("\nTOP RESULTS:")
    for result in output["results"][:5]:
        metadata = result.get("metadata", {}) or {}

        print({
            "rank": result.get("rank"),
            "score": result.get("score"),
            "company": metadata.get("company"),
            "fiscal_year": metadata.get("fiscal_year"),
            "section_type": metadata.get("section_type"),
            "form_item": metadata.get("form_item"),
            "section_title": result.get("section_title"),
        })


if __name__ == "__main__":
    main()
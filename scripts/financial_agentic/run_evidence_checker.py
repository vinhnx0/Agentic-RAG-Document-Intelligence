# run_evidence_checker.py

from agentic.evidence_checker import EvidenceSufficiencyChecker
from agentic.metadata_filter import MetadataFilterBuilder
from agentic.query_planner import RuleBasedQueryPlanner
from reranking.pipeline import RerankingPipeline


CONFIG_PATH = "configs/corpora/financial_reports.yaml"


def main() -> None:
    query = "What was Apple’s net sales in 2023?"

    planner = RuleBasedQueryPlanner()
    filter_builder = MetadataFilterBuilder()
    checker = EvidenceSufficiencyChecker()

    query_plan = planner.plan(query)
    metadata_filters = filter_builder.build(query_plan)

    reranking_pipeline = RerankingPipeline(CONFIG_PATH)

    retrieval_output = reranking_pipeline.retrieval_pipeline.run(
        query=query,
        metadata_filters=metadata_filters,
    )

    reranker = reranking_pipeline.reranker_registry.get_reranker()
    reranked_results = reranker.rerank(
        query=query,
        results=retrieval_output["results"],
        final_top_k=5,
    )

    evidence_check = checker.check(
        query_plan=query_plan,
        chunks=reranked_results,
    )

    print("QUERY:")
    print(query)

    print("\nQUERY PLAN:")
    print(query_plan.to_dict())

    print("\nMETADATA FILTERS:")
    print(metadata_filters)

    print("\nRERANKED RESULT COUNT:")
    print(len(reranked_results))

    print("\nEVIDENCE CHECK:")
    print(evidence_check.to_dict())


if __name__ == "__main__":
    main()
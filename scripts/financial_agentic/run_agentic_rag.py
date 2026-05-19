# run_agentic_rag.py

from agentic.pipeline import AgenticRAGPipeline
from utils.io import to_jsonable


CONFIG_PATH = "configs/corpora/financial_reports.yaml"


def main() -> None:
    query = "What market risks did Apple report in 2023?"

    pipeline = AgenticRAGPipeline(CONFIG_PATH)
    answer = pipeline.run(query)

    output = to_jsonable(answer)

    print("QUERY:")
    print(output["query"])

    print("\nANSWER:")
    print(output["answer"][:1000])

    print("\nCITATIONS:")
    for citation in output["citations"]:
        print(citation)

    print("\nQUERY PLAN:")
    print(output["metadata"]["query_plan"])

    print("\nMETADATA FILTERS:")
    print(output["metadata"]["metadata_filters"])

    print("\nEVIDENCE CHECK:")
    print(output["metadata"]["evidence_check"])


if __name__ == "__main__":
    main()
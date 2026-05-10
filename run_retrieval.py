# run_retrieval.py

from __future__ import annotations

import argparse
from typing import Any

from retrieval.pipeline import RetrievalPipeline
from utils.io import ensure_stage_output_dir, write_json


CONFIG_PATH = "configs/corpora/tech_docs.yaml"


DEFAULT_QUERY = "How do I create a Qdrant collection?"


def build_summary(retrieval_output: dict[str, Any]) -> dict[str, Any]:
    return {
        "query": retrieval_output["query"],
        "collection_name": retrieval_output["collection_name"],
        "embedding_model": retrieval_output["embedding_model"],
        "top_k": retrieval_output["top_k"],
        "score_threshold": retrieval_output["score_threshold"],
        "result_count": retrieval_output["result_count"],
        "results_preview": [
            {
                "rank": result["rank"],
                "score": result["score"],
                "chunk_id": result["chunk_id"],
                "doc_id": result["doc_id"],
                "section_title": result["section_title"],
                "section_path": result["section_path"],
                "source_path": result["source_path"],
                "text_preview": result["text"][:250] if result["text"] else "",
            }
            for result in retrieval_output["results"]
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run retrieval smoke test against Qdrant vector store."
    )
    parser.add_argument(
        "--query",
        type=str,
        default=DEFAULT_QUERY,
        help="Query text to retrieve relevant chunks for.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=None,
        help="Number of retrieval results to return.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    pipeline = RetrievalPipeline(CONFIG_PATH)
    retrieval_output = pipeline.run(
        query=args.query,
        top_k=args.top_k,
    )

    processed_data_dir = pipeline.get_processed_data_dir()
    output_dir = ensure_stage_output_dir(processed_data_dir, "retrieval")

    results_path = output_dir / "retrieval_results.json"
    summary_path = output_dir / "retrieval_summary.json"

    write_json(results_path, retrieval_output)
    write_json(summary_path, build_summary(retrieval_output))

    print(f"Query: {retrieval_output['query']}")
    print(f"Retrieved {retrieval_output['result_count']} results")
    print(f"Saved outputs to: {output_dir}")
    print(f"Results file: {results_path}")
    print(f"Summary file: {summary_path}")

    for result in retrieval_output["results"]:
        print(
            f"\nRank {result['rank']} | "
            f"Score: {result['score']:.4f} | "
            f"Section: {result['section_title']}"
        )
        print(f"Source: {result['source_path']}")
        preview = result["text"][:300].replace("\n", " ") if result["text"] else ""
        print(f"Preview: {preview}")


if __name__ == "__main__":
    main()
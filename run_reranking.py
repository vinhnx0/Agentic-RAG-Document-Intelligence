# run_reranking.py

from __future__ import annotations

import argparse
from typing import Any

from reranking.pipeline import RerankingPipeline
from utils.io import ensure_stage_output_dir, write_json


CONFIG_PATH = "configs/corpora/tech_docs.yaml"


DEFAULT_QUERY = "How do I create a Qdrant collection?"


def build_summary(reranking_output: dict[str, Any]) -> dict[str, Any]:
    return {
        "query": reranking_output["query"],
        "collection_name": reranking_output["collection_name"],
        "embedding_model": reranking_output["embedding_model"],
        "reranker_model": reranking_output["reranker_model"],
        "retrieval_top_k": reranking_output["retrieval_top_k"],
        "final_top_k": reranking_output["final_top_k"],
        "retrieval_result_count": reranking_output["retrieval_result_count"],
        "reranked_result_count": reranking_output["reranked_result_count"],
        "reranked_results_preview": [
            {
                "rerank_rank": result["rerank_rank"],
                "rerank_score": result["rerank_score"],
                "retrieval_rank": result["retrieval_rank"],
                "retrieval_score": result["retrieval_score"],
                "chunk_id": result["chunk_id"],
                "doc_id": result["doc_id"],
                "section_title": result["section_title"],
                "section_path": result["section_path"],
                "source_path": result["source_path"],
                "text_preview": result["text"][:250] if result["text"] else "",
            }
            for result in reranking_output["reranked_results"]
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run retrieval + reranking smoke test."
    )
    parser.add_argument(
        "--query",
        type=str,
        default=DEFAULT_QUERY,
        help="Query text to retrieve and rerank relevant chunks for.",
    )
    parser.add_argument(
        "--retrieval-top-k",
        type=int,
        default=None,
        help="Number of retrieval candidates before reranking.",
    )
    parser.add_argument(
        "--final-top-k",
        type=int,
        default=None,
        help="Number of final reranked results.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    pipeline = RerankingPipeline(CONFIG_PATH)
    reranking_output = pipeline.run(
        query=args.query,
        retrieval_top_k=args.retrieval_top_k,
        final_top_k=args.final_top_k,
    )

    processed_data_dir = pipeline.get_processed_data_dir()
    output_dir = ensure_stage_output_dir(processed_data_dir, "reranking")

    results_path = output_dir / "reranking_results.json"
    summary_path = output_dir / "reranking_summary.json"

    write_json(results_path, reranking_output)
    write_json(summary_path, build_summary(reranking_output))

    print(f"Query: {reranking_output['query']}")
    print(
        "Retrieved "
        f"{reranking_output['retrieval_result_count']} candidates, "
        f"reranked to {reranking_output['reranked_result_count']} results"
    )
    print(f"Saved outputs to: {output_dir}")
    print(f"Results file: {results_path}")
    print(f"Summary file: {summary_path}")

    for result in reranking_output["reranked_results"]:
        print(
            f"\nRerank {result['rerank_rank']} | "
            f"Rerank score: {result['rerank_score']:.4f} | "
            f"Retrieval rank: {result['retrieval_rank']} | "
            f"Retrieval score: {result['retrieval_score']:.4f}"
        )
        print(f"Section: {result['section_title']}")
        print(f"Source: {result['source_path']}")
        preview = result["text"][:300].replace("\n", " ") if result["text"] else ""
        print(f"Preview: {preview}")


if __name__ == "__main__":
    main()
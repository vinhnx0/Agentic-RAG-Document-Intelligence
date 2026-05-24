# run_rag.py

from __future__ import annotations
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

import argparse
from typing import Any

from rag.pipeline import RAGPipeline
from utils.io import ensure_stage_output_dir, to_jsonable, write_json


CONFIG_PATH = "configs/corpora/financial_reports.yaml"


DEFAULT_QUERY = "How did Apple describe competition?"


def build_summary(rag_output: dict[str, Any]) -> dict[str, Any]:
    return {
        "query": rag_output["query"],
        "answer_preview": rag_output["answer"][:500],
        "citation_count": len(rag_output["citations"]),
        "context_chunk_count": len(rag_output["context_chunks"]),
        "metadata": rag_output["metadata"],
        "citations_preview": [
            {
                "citation_id": citation["citation_id"],
                "chunk_id": citation["chunk_id"],
                "doc_id": citation["doc_id"],
                "source_path": citation["source_path"],
                "section_title": citation["section_title"],
                "retrieval_score": citation["retrieval_score"],
                "rerank_score": citation["rerank_score"],
            }
            for citation in rag_output["citations"]
        ],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run RAG answer generation with citations."
    )
    parser.add_argument(
        "--query",
        type=str,
        default=DEFAULT_QUERY,
        help="Question to answer from the document corpus.",
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
        help="Number of final reranked context chunks.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    pipeline = RAGPipeline(CONFIG_PATH)
    rag_answer = pipeline.run(
        query=args.query,
        retrieval_top_k=args.retrieval_top_k,
        final_top_k=args.final_top_k,
    )

    rag_output = to_jsonable(rag_answer)

    processed_data_dir = pipeline.get_processed_data_dir()
    output_dir = ensure_stage_output_dir(processed_data_dir, "rag")

    results_path = output_dir / "rag_answer.json"
    summary_path = output_dir / "rag_summary.json"

    write_json(results_path, rag_output)
    write_json(summary_path, build_summary(rag_output))

    print(f"Query: {rag_output['query']}")
    print(f"Generated answer with {len(rag_output['citations'])} citations")
    print(f"Saved outputs to: {output_dir}")
    print(f"Answer file: {results_path}")
    print(f"Summary file: {summary_path}")

    print("\nAnswer:\n")
    print(rag_output["answer"])

    print("\nCitations:")
    for citation in rag_output["citations"]:
        print(
            f"[{citation['citation_id']}] "
            f"{citation['source_path']} | "
            f"Section: {citation['section_title']} | "
            f"Chunk: {citation['chunk_id']}"
        )


if __name__ == "__main__":
    main()
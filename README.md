# AI Document Intelligence System (Agentic RAG)

A production-oriented Retrieval-Augmented Generation (RAG) system for ingesting structured documents, retrieving relevant context, reranking results, and generating citation-based answers.

The project focuses on **reliable document retrieval**, not just chatbot generation. The current baseline is tested on technical documentation, with public financial reports planned as the main real-world use case.

## Key Features

- Modular document pipeline: ingestion → parsing → chunking → embedding → retrieval → reranking → RAG → evaluation
- Heading-based document parsing and section-aware chunking
- Qdrant vector database integration with metadata-rich payloads
- Cross-encoder reranking for improved context selection
- Extractive RAG answers with source citations
- Evaluation pipeline with retrieval, reranking, and citation metrics

## Tech Stack

- Python
- SentenceTransformers
- Qdrant
- Cross-encoder reranker
- YAML configuration
- JSON stage artifacts

## Current Status

The end-to-end CLI baseline is complete for Dataset A: `tech_docs`.

Completed:

- Ingestion for Markdown and text documents
- Structured parsing with headings and section paths
- Section-aware chunking
- Embedding with `BAAI/bge-small-en-v1.5`
- Qdrant vector indexing
- Semantic retrieval
- Reranking with `BAAI/bge-reranker-base`
- Extractive answer generation with citations
- Baseline evaluation on 15 test queries

## Pipeline Overview

```text
Raw Documents
  → Ingestion
  → Parsing
  → Section-aware Chunking
  → Embedding
  → Qdrant Vector Store
  → Retrieval
  → Reranking
  → Citation-based RAG Answer
  → Evaluation
```

## Baseline Evaluation

Evaluation was run on 15 manually designed technical-documentation queries, covering keyword-based, section-based, and paraphrased questions.

| Component | Metric | Score |
|---|---:|---:|
| Retrieval | Hit@1 | 0.73 |
| Retrieval | Hit@5 | 1.00 |
| Retrieval | Hit@10 | 1.00 |
| Retrieval | MRR | 0.86 |
| Reranking | Hit@1 | 0.73 |
| Reranking | Hit@5 | 1.00 |
| Reranking | MRR | 0.83 |
| RAG | Avg. citation coverage | 1.00 |
| RAG | Avg. citation count | 5.00 |

The baseline confirms that the system can consistently retrieve the expected documentation within the top 5 results. The next improvement is to make embeddings more structure-aware by including section titles and section paths in the embedding input.

Full evaluation outputs:

- `data/processed/tech_docs/evaluation/evaluation_summary.json`
- `data/processed/tech_docs/evaluation/evaluation_report.json`

## Project Structure

```text
configs/          # corpus configuration
ingestion/        # document loading
parsing/          # structured parsing
chunking/         # section-aware chunking
embedding/        # embedding generation
vectorstore/      # Qdrant integration
retrieval/        # vector retrieval
reranking/        # cross-encoder reranking
rag/              # answer generation and citations
evaluation/       # metrics and evaluation pipeline
data/             # raw, processed, and evaluation data
run_*.py          # CLI runners for each pipeline stage
```

## Setup

Create virtual environment:
```bash
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Start Qdrant:

```bash
docker run -p 6333:6333 -p 6334:6334 qdrant/qdrant
```

## Run the Pipeline

Run each stage in order:

```bash
python run_ingestion.py
python run_parsing.py
python run_chunking.py
python run_embedding.py
python run_vectorstore.py
python run_retrieval.py
python run_reranking.py
python run_rag.py
python run_evaluation.py
```

## Current Limitations

- PDF ingestion and parsing are not fully implemented yet.
- Current embedding baseline mainly uses chunk body text; section titles and paths are stored as metadata but not fully included in embedding input.
- Current answer generation is extractive and does not call an external LLM yet.
- Dataset A is small and mainly used to validate the architecture.

## Next Steps

- Add heading-aware embedding input using section title, section path, document title, and chunk text.
- Create a separate Qdrant collection for the improved retrieval version.
- Re-run the same evaluation set and compare before/after retrieval metrics.
- Expand the system to Dataset B: public financial reports and SEC 10-K filings.

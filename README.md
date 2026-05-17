# AI Document Intelligence System (Agentic RAG)

Modular RAG pipeline for financial document intelligence with metadata-aware retrieval and evaluation.


## Project Goal

Build a production-oriented Retrieval-Augmented Generation (RAG) system for complex documents, starting from architecture validation on technical documentation and moving toward real-world financial report intelligence using SEC 10-K filings.

The project focuses on:

* modular AI engineering architecture
* metadata-aware retrieval
* evaluation-driven iteration
* production-oriented document processing pipelines


## Current Status

### Completed

* Modular ingestion → retrieval → evaluation pipeline
* Section-aware chunking
* Metadata-enriched embedding
* Qdrant vector database integration
* Cross-encoder reranking
* Extractive RAG with citation output
* Evaluation pipeline with Hit@K, MRR, and citation coverage
* SEC-aware financial report parser
* Baseline Financial RAG evaluation

### In Progress

* Query Planner
* Metadata-Aware Retrieval
* Evidence Sufficiency Checker


## Architecture Overview

```text
Raw Documents
  → Ingestion
  → Parsing
  → Section-aware Chunking
  → Embedding
  → Qdrant Vector Store
  → Retrieval
  → Reranking
  → Extractive RAG
  → Evaluation
```

Current system design is modular and config-driven to support experimentation across datasets and retrieval strategies.


## Dataset Strategy

### Dataset A — Technical Documentation (Architecture Validation)

Dataset A was used to validate the modular RAG architecture, retrieval pipeline, and evaluation framework before moving to financial documents.

A small 15-query evaluation compared:

* baseline embedding
* heading-aware embedding

Results showed that adding section metadata improved retrieval quality, especially Hit@1 and MRR.

| Version       | Retrieval Hit@1 | Retrieval MRR | Reranking Hit@1 |
| ------------- | --------------: | ------------: | --------------: |
| Baseline      |            0.73 |          0.86 |            0.73 |
| Heading-aware |            0.87 |          0.93 |            0.80 |

Dataset A is mainly used as an architecture proof and evaluation baseline.


### Dataset B — SEC 10-K Financial Reports (Main Portfolio Use Case)

Dataset B is the primary real-world use case of the project.

Current dataset:

* Apple SEC 10-K filings (2021–2025)

Current financial pipeline includes:

* Docling PDF parsing
* SEC-aware section parsing
* Section-aware chunking
* Metadata-enriched embedding
* Metadata-enriched reranking
* Financial retrieval evaluation

The project is currently building toward:

* Baseline Financial RAG
* Agentic Metadata-Aware Financial RAG


## Baseline Financial RAG

The current financial pipeline uses:

* semantic vector retrieval
* cross-encoder reranking
* metadata-enriched embedding
* extractive RAG answer generation
* citation-based output

Financial metadata currently used across the pipeline includes:

* company
* fiscal year
* SEC form item
* section type
* subsection headings

Example:

```text id="r2o6gs"
Item 1A → Risk Factors
Item 1C → Cybersecurity
Item 7 → Management Discussion
Item 7A → Market Risk
Item 8 → Financial Statements
```

This baseline establishes the “before” system for later comparison with the agentic retrieval layer.


## Planned Agentic Layer

The next stage of the project focuses on metadata-aware financial retrieval.

### 1. Query Planner

Converts natural language questions into structured retrieval intent.

Example:

```json id="q8z7kh"
{
  "company": "Apple",
  "years": [2025],
  "form_items": ["Item 7A"],
  "section_types": ["market_risk"]
}
```

### 2. Metadata-Aware Retrieval

Uses query metadata to narrow retrieval candidates before vector search.

Goal:

* reduce wrong-section retrieval
* improve year-specific retrieval
* improve SEC section targeting

### 3. Evidence Sufficiency Checker

Detects when retrieved evidence is insufficient instead of forcing an answer.

Goal:

* reduce hallucinated responses
* improve reliability for financial QA


## Evaluation Strategy

The project uses retrieval-focused evaluation instead of only qualitative demos.

Current metrics:

* Hit@1
* Hit@5
* Hit@10
* MRR (Mean Reciprocal Rank)
* citation coverage

Evaluation uses manually designed financial-report queries covering:

* revenue questions
* risk-factor questions
* management discussion questions
* multi-year comparison questions
* insufficient-evidence questions

Main comparison direction:

```text id="13z3w1"
Baseline Financial RAG
vs
Agentic Metadata-Aware Financial RAG
```


## Current Baseline Financial Results

| Stage     | Hit@1 | Hit@5 |  MRR |
| --------- | ----: | ----: | ---: |
| Retrieval |  0.54 |  0.88 | 0.69 |
| Reranking |  0.77 |  0.94 | 0.85 |

Current remaining failure cases are mostly:

* wrong SEC section retrieval
* cybersecurity section targeting
* market-risk section targeting
* multi-year retrieval reasoning

These failures directly motivate the next metadata-aware retrieval phase.


## Tech Stack

### Core

* Python
* PyYAML
* Dataclasses

### Document Processing

* Docling
* Custom SEC-aware parser

### Retrieval & Embedding

* SentenceTransformers
* CrossEncoder reranking
* Qdrant Vector Database

### Evaluation

* Hit@K
* MRR
* Citation coverage evaluation

### Infrastructure

* Docker
* FastAPI (planned)


## How to Run

```bash id="e0vtj7"
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt

docker run -p 6333:6333 -p 6334:6334 qdrant/qdrant

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


## Project Structure

```text id="xum4cw"
configs/
data/
ingestion/
parsing/
chunking/
embedding/
retrieval/
reranking/
rag/
evaluation/
schemas/
utils/
```


## Current Limitations

* Current RAG generator is extractive, not generative
* Query planner is still rule-based (in progress)
* Metadata-aware filtering is not fully implemented yet
* Evaluation currently focuses on Apple SEC filings only
* No production deployment yet


## Next Steps

- Query Planner
- Metadata-Aware Retrieval
- Evidence Sufficiency Checker
- FastAPI serving layer


## Engineering Decisions

### Why SEC-aware parsing?

SEC 10-K filings contain highly structured sections (Item 1A, Item 7, Item 8, etc.). Preserving this structure improves retrieval quality and enables metadata-aware search.

### Why metadata-enriched embedding and reranking?

Pure semantic retrieval often retrieves related but incorrect SEC sections. Metadata enrichment improves section targeting and year-specific retrieval.

### Why evaluation-first development?

The project tracks retrieval quality using Hit@K and MRR before adding agentic features, allowing measurable before/after comparisons.


## Portfolio Highlights

* Built modular end-to-end RAG pipeline from ingestion to evaluation
* Implemented SEC-aware financial document parser
* Added metadata-enriched embedding and reranking
* Integrated Qdrant vector search with reranking pipeline
* Built retrieval evaluation framework using Hit@K and MRR
* Created baseline for future agentic metadata-aware retrieval comparison
* Designed financial-document pipeline around real SEC 10-K filings
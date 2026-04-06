# AI Document Intelligence System (Agentic RAG-based)

## Overview

This project aims to build a production-style AI system that can:

- Ingest documents (PDF, markdown, text)
- Extract structure (sections, layout)
- Convert documents into structured knowledge
- Perform retrieval-augmented generation (RAG)
- Provide answers with citations
- Evaluate retrieval and answer quality

The focus is on **building a clean, modular, and scalable pipeline**, rather than a quick demo.


## Why This Project

Most RAG projects skip directly to LLMs.

This project focuses on:
- data pipeline quality
- document structure understanding
- retrieval reliability
- system design for real-world use cases

The long-term goal is to support:
- financial documents
- enterprise knowledge systems
- agentic reasoning workflows


## Current Scope

This project is being developed with a deliberate, production-oriented approach:

- Building the foundation pipeline first (not jumping directly into LLM/RAG)
- Using Dataset A (technical documentation) to validate system architecture and workflow
- Planning Dataset B (financial reports) as the main real-world production use case


## Current Development Status

**Phase:** Data Ingestion (Completed)  
**Next Phase:** Markdown Parsing


## Implemented Components

### 1. Schema Layer
- `RawDocument` (active)
- `ParsedDocument`, `DocumentSection`, `DocumentChunk` (planned)

### 2. Ingestion System
- Base loader abstraction
- Markdown loader
- Text loader
- Loader registry pattern

### 3. Ingestion Pipeline
- Config-driven ingestion
- Recursive file discovery
- File-type filtering
- Normalized document output

### 4. Dataset (Current)
- Technical documentation corpus
  - FastAPI docs
  - Qdrant docs
- ~17 markdown documents


## Project Structure

```
Agentic-RAG-Document-Intelligence/
├── configs/
│   └── corpora/
│       └── tech_docs.yaml
├── schemas/
│   └── documents.py
├── ingestion/
│   ├── loaders.py
│   └── pipeline.py
├── data/
│   └── raw/
│       └── tech_docs/
├── run_ingestion.py
├── requirements.txt
└── README.md

````


## Setup

### 1. Create virtual environment
```bash
python -m venv .venv
````

### 2. Activate environment

Windows (PowerShell):

```bash
.venv\Scripts\Activate.ps1
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```


## Run Ingestion

```bash
python run_ingestion.py
```

This will:

* scan `data/raw/tech_docs`
* load markdown/text files
* output normalized documents


## Current Pipeline Flow

```
Raw Documents (.md, .txt)
    ↓
Loaders (MarkdownLoader / TextLoader)
    ↓
IngestionPipeline
    ↓
RawDocument objects
```


## Known Limitations

* Markdown content is not yet parsed into sections
* Titles may contain anchor artifacts (e.g. `{ #... }`)
* HTML/style noise is not removed
* No document versioning or stable IDs yet
* No persistence or indexing layer


## Next Steps

* Implement markdown parser:

  * extract headings
  * build section hierarchy
* Add structured parsing output (`ParsedDocument`)
* Implement section-aware chunking
* Add embeddings and vector database (Qdrant)
* Build retrieval + reranking
* Implement RAG pipeline with citations


## Design Principles

* Modular pipeline (clear separation of stages)
* Config-driven behavior
* Strong data contracts (schemas)
* Avoid premature optimization
* Build foundation before adding LLM layers
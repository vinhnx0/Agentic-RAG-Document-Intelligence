# Architecture

This project is organized around two main flows:

1. Offline indexing pipeline
2. Online query pipeline

---

## 1. Offline Indexing Pipeline

This pipeline prepares SEC 10-K filings for retrieval.

```text
SEC 10-K PDFs
  ↓
Ingestion
  ↓
SEC-aware Parsing
  ↓
Section-aware Chunking
  ↓
Metadata-Enriched Embedding
  ↓
Qdrant Vector Store
```

Main purpose:

```text
Convert raw financial reports into searchable document chunks
with useful metadata such as company, fiscal year, SEC item,
section title, and section type.
```

---

## 2. Online Query Pipeline

This pipeline answers user questions using baseline or agentic RAG.

```text
User Question
  ↓
Flask Demo UI / FastAPI Endpoint
  ↓
Agentic RAG Pipeline
  ├── Query Planner
  │     └── extracts intent, company, year, metric, and section type
  │
  ├── Metadata Filter Builder
  │     └── converts query plan into Qdrant metadata filters
  │
  ├── Qdrant Retrieval
  │     └── retrieves semantically relevant chunks after metadata filtering
  │
  ├── Cross-Encoder Reranking
  │     └── reranks retrieved chunks using query-document relevance
  │
  ├── Evidence Sufficiency Checker
  │     └── checks whether retrieved evidence supports the question
  │
  └── Extractive RAG + Citations
        └── returns answer with source-backed citations
```

---

## 3. Baseline vs Agentic Flow

```text
Baseline Financial RAG
  ↓
Query
  ↓
Vector Retrieval
  ↓
Cross-Encoder Reranking
  ↓
Extractive Answer + Citations
```

```text
Agentic Metadata-Aware Financial RAG
  ↓
Query
  ↓
Query Planner
  ↓
Metadata Filter Builder
  ↓
Filtered Vector Retrieval
  ↓
Cross-Encoder Reranking
  ↓
Evidence Sufficiency Checker
  ↓
Extractive Answer + Citations
```

---

## 4. System Layers

```text
Interface Layer
├── Flask demo UI
└── FastAPI serving layer

Agentic Layer
├── Query planner
├── Metadata filter builder
├── Evidence sufficiency checker
└── Agentic RAG pipeline

RAG Layer
├── Retrieval
├── Reranking
├── Citation builder
└── Extractive answer generator

Data Layer
├── SEC 10-K filings
├── Processed chunks
├── Embedded vectors
└── Qdrant vector database

Evaluation Layer
├── Baseline evaluation
├── Agentic evaluation
├── Hit@K / MRR
├── Citation coverage
└── Insufficient-evidence accuracy
```
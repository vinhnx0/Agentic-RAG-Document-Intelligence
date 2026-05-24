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

The project now compares:
- Baseline Financial RAG
- Agentic Metadata-Aware Financial RAG


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

## Current Agentic Financial Results

| Metric | Score |
| ------ | ----: |
| Retrieval Hit@1 | 0.88 |
| Retrieval Hit@5 | 0.98 |
| Retrieval Hit@10 | 0.98 |
| MRR | 0.93 |
| Citation Coverage | 0.92 |
| Average Citation Count | 4.31 |

Agentic results include query planning, metadata-aware filtering, reranking, evidence sufficiency checking, and citation-backed answer generation.
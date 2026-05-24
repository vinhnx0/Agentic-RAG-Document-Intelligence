# Engineering Decisions

## Why SEC-aware parsing?

SEC 10-K filings contain highly structured sections (Item 1A, Item 7, Item 8, etc.). Preserving this structure improves retrieval quality and enables metadata-aware search.

## Why metadata-enriched embedding and reranking?

Pure semantic retrieval often retrieves related but incorrect SEC sections. Metadata enrichment improves section targeting and year-specific retrieval.

## Why evaluation-first development?

The project tracks retrieval quality using Hit@K and MRR before adding agentic features, allowing measurable before/after comparisons.
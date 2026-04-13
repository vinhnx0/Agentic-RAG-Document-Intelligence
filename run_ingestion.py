from ingestion.pipeline import IngestionPipeline


def main() -> None:
    pipeline = IngestionPipeline("configs/corpora/tech_docs.yaml")
    documents = pipeline.run()

    print(f"Ingested {len(documents)} documents\n")

    for doc in documents:
        print("=" * 80)
        print(f"doc_id      : {doc.doc_id}")
        print(f"title       : {doc.title}")
        print(f"type        : {doc.source_type}")
        print(f"source_path : {doc.source_path}")
        print(f"preview     : {doc.raw_text[:150].replace(chr(10), ' ')}")
        print()


if __name__ == "__main__":
    main()
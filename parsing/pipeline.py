# parsing/pipeline.py

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from parsing.parsers import ParserRegistry
from schemas.documents import ParsedDocument, RawDocument


class ParsingPipeline:
    def __init__(self, config_path: str | Path) -> None:
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.registry = ParserRegistry()

    def _load_config(self) -> dict[str, Any]:
        with self.config_path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def run(self, documents: list[RawDocument]) -> list[ParsedDocument]:
        parser_type = self.config.get("parser", {}).get("type")

        if not parser_type:
            raise ValueError("Missing parser.type in config")

        if parser_type != "markdown_structured":
            raise ValueError(f"Unsupported parser.type: {parser_type}")

        parsed_documents: list[ParsedDocument] = []

        for document in documents:
            parser = self.registry.get_parser(document)
            parsed_doc = parser.parse(document)
            parsed_documents.append(parsed_doc)

        return parsed_documents
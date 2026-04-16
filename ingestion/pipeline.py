# ingestion/pipeline.py

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from ingestion.loaders import LoaderRegistry
from schemas.documents import RawDocument


class IngestionPipeline:
    def __init__(self, config_path: str | Path) -> None:
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.registry = LoaderRegistry()

    def _load_config(self) -> dict[str, Any]:
        with self.config_path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def run(self) -> list[RawDocument]:
        corpus_name = self.config["corpus_name"]
        raw_data_dir = Path(self.config["raw_data_dir"])
        recursive = self.config.get("ingestion", {}).get("recursive", True)
        encoding = self.config.get("ingestion", {}).get("encoding", "utf-8")
        allowed_types = {
            ext.lower().lstrip(".")
            for ext in self.config.get("file_types", [])
        }

        if not raw_data_dir.exists():
            raise FileNotFoundError(f"Directory not found: {raw_data_dir}")

        file_paths = self._discover_files(raw_data_dir, recursive, allowed_types)

        documents: list[RawDocument] = []
        for file_path in file_paths:
            loader = self.registry.get_loader(file_path)
            doc = loader.load(
                file_path=file_path,
                corpus_name=corpus_name,
                encoding=encoding,
            )
            documents.append(doc)

        return documents

    def get_processed_data_dir(self) -> Path:
        processed_data_dir = self.config.get("processed_data_dir")
        if not processed_data_dir:
            raise KeyError(
                "Missing 'processed_data_dir' in corpus config. "
                "Please add it to the YAML file."
            )
        return Path(processed_data_dir)

    @staticmethod
    def _discover_files(
        raw_data_dir: Path,
        recursive: bool,
        allowed_types: set[str],
    ) -> list[Path]:
        pattern = "**/*" if recursive else "*"
        files = [
            path
            for path in raw_data_dir.glob(pattern)
            if path.is_file() and path.suffix.lower().lstrip(".") in allowed_types
        ]
        return sorted(files)
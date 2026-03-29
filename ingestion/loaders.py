from __future__ import annotations

import hashlib
import re
from abc import ABC, abstractmethod
from pathlib import Path

from schemas.documents import RawDocument


FIRST_HEADING_PATTERN = re.compile(r"^\s*#\s+(.+?)\s*$", re.MULTILINE)


class BaseLoader(ABC):
    supported_extensions: set[str] = set()

    def can_load(self, file_path: Path) -> bool:
        return file_path.suffix.lower().lstrip(".") in self.supported_extensions

    @abstractmethod
    def load(
        self,
        file_path: Path,
        corpus_name: str,
        encoding: str = "utf-8",
    ) -> RawDocument:
        raise NotImplementedError


class MarkdownLoader(BaseLoader):
    supported_extensions = {"md", "markdown"}

    def load(
        self,
        file_path: Path,
        corpus_name: str,
        encoding: str = "utf-8",
    ) -> RawDocument:
        raw_text = file_path.read_text(encoding=encoding)
        title = self._extract_title(raw_text, file_path)
        doc_id = self._build_doc_id(corpus_name, file_path)

        return RawDocument(
            doc_id=doc_id,
            corpus=corpus_name,
            source_type="markdown",
            source_path=str(file_path),
            file_name=file_path.name,
            title=title,
            raw_text=raw_text,
            metadata={
                "extension": file_path.suffix.lower(),
                "loader": "MarkdownLoader",
            },
        )

    @staticmethod
    def _extract_title(raw_text: str, file_path: Path) -> str:
        match = FIRST_HEADING_PATTERN.search(raw_text)
        if match:
            return match.group(1).strip()
        return file_path.stem.replace("_", " ").replace("-", " ").strip()

    @staticmethod
    def _build_doc_id(corpus_name: str, file_path: Path) -> str:
        digest = hashlib.sha1(str(file_path).encode("utf-8")).hexdigest()[:12]
        return f"{corpus_name}_{file_path.stem}_{digest}"


class TextLoader(BaseLoader):
    supported_extensions = {"txt"}

    def load(
        self,
        file_path: Path,
        corpus_name: str,
        encoding: str = "utf-8",
    ) -> RawDocument:
        raw_text = file_path.read_text(encoding=encoding)
        title = file_path.stem.replace("_", " ").replace("-", " ").strip()
        doc_id = self._build_doc_id(corpus_name, file_path)

        return RawDocument(
            doc_id=doc_id,
            corpus=corpus_name,
            source_type="text",
            source_path=str(file_path),
            file_name=file_path.name,
            title=title,
            raw_text=raw_text,
            metadata={
                "extension": file_path.suffix.lower(),
                "loader": "TextLoader",
            },
        )

    @staticmethod
    def _build_doc_id(corpus_name: str, file_path: Path) -> str:
        digest = hashlib.sha1(str(file_path).encode("utf-8")).hexdigest()[:12]
        return f"{corpus_name}_{file_path.stem}_{digest}"


class LoaderRegistry:
    def __init__(self) -> None:
        self._loaders = [
            MarkdownLoader(),
            TextLoader(),
        ]

    def get_loader(self, file_path: Path) -> BaseLoader:
        for loader in self._loaders:
            if loader.can_load(file_path):
                return loader
        raise ValueError(f"No loader found for file: {file_path}")
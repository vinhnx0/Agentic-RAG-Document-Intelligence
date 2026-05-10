# chunking/chunkers.py

from __future__ import annotations

import hashlib
import re
from abc import ABC, abstractmethod

from schemas.documents import DocumentChunk, DocumentSection, ParsedDocument


TOKEN_PATTERN = re.compile(r"\S+")


class BaseChunker(ABC):
    @abstractmethod
    def chunk(self, document: ParsedDocument) -> list[DocumentChunk]:
        raise NotImplementedError


class SectionTokenChunker(BaseChunker):
    def __init__(
        self,
        max_tokens: int = 350,
        overlap_tokens: int = 50,
        min_chunk_chars: int = 150,
    ) -> None:
        if max_tokens <= 0:
            raise ValueError("max_tokens must be > 0")
        if overlap_tokens < 0:
            raise ValueError("overlap_tokens must be >= 0")
        if overlap_tokens >= max_tokens:
            raise ValueError("overlap_tokens must be < max_tokens")
        if min_chunk_chars < 0:
            raise ValueError("min_chunk_chars must be >= 0")

        self.max_tokens = max_tokens
        self.overlap_tokens = overlap_tokens
        self.min_chunk_chars = min_chunk_chars

    def chunk(self, document: ParsedDocument) -> list[DocumentChunk]:
        chunks: list[DocumentChunk] = []
        chunk_index = 0

        for section in document.sections:
            section_chunks = self._chunk_section(
                document=document,
                section=section,
                start_chunk_index=chunk_index,
            )
            chunks.extend(section_chunks)
            chunk_index += len(section_chunks)

        return chunks

    def _chunk_section(
        self,
        document: ParsedDocument,
        section: DocumentSection,
        start_chunk_index: int,
    ) -> list[DocumentChunk]:
        section_text = section.text.strip()

        if len(section_text) < self.min_chunk_chars:
            return []

        tokens = self._tokenize(section_text)
        if not tokens:
            return []

        windows = self._split_tokens(tokens)

        chunks: list[DocumentChunk] = []
        for offset, window_tokens in enumerate(windows):
            chunk_text = " ".join(window_tokens).strip()
            if not chunk_text:
                continue

            chunk_index = start_chunk_index + offset
            section_path = list(section.metadata.get("heading_path", [section.title]))

            chunk = DocumentChunk(
                chunk_id=self._build_chunk_id(document.doc_id, section.section_id, chunk_index),
                doc_id=document.doc_id,
                corpus=document.corpus,
                text=chunk_text,
                chunk_index=chunk_index,
                section_title=section.title,
                section_path=section_path,
                metadata={
                    "source_type": document.source_type,
                    "source_path": document.source_path,
                    "section_id": section.section_id,
                    "section_level": section.level,
                    "parent_section_id": section.parent_section_id,
                    "token_count_estimate": len(window_tokens),
                },
            )
            chunks.append(chunk)

        return chunks

    @staticmethod
    def _tokenize(text: str) -> list[str]:
        return TOKEN_PATTERN.findall(text)

    def _split_tokens(self, tokens: list[str]) -> list[list[str]]:
        if len(tokens) <= self.max_tokens:
            return [tokens]

        windows: list[list[str]] = []
        step = self.max_tokens - self.overlap_tokens
        start = 0

        while start < len(tokens):
            end = start + self.max_tokens
            window = tokens[start:end]
            if not window:
                break

            windows.append(window)

            if end >= len(tokens):
                break

            start += step

        return windows

    @staticmethod
    def _build_chunk_id(doc_id: str, section_id: str, chunk_index: int) -> str:
        raw_key = f"{doc_id}::{section_id}::{chunk_index}"
        digest = hashlib.sha1(raw_key.encode("utf-8")).hexdigest()[:12]
        return f"{doc_id}_chunk_{digest}"


class ChunkerRegistry:
    def __init__(
        self,
        max_tokens: int,
        overlap_tokens: int,
        min_chunk_chars: int,
    ) -> None:
        self._chunker = SectionTokenChunker(
            max_tokens=max_tokens,
            overlap_tokens=overlap_tokens,
            min_chunk_chars=min_chunk_chars,
        )

    def get_chunker(self) -> BaseChunker:
        return self._chunker
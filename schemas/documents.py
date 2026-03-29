# schemas/documents.py

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True)
class RawDocument:
    """
    Output of ingestion stage.
    Represents a source file normalized into a standard internal format.
    """
    doc_id: str
    corpus: str
    source_type: str           # e.g. "markdown", "text", later "pdf"
    source_path: str
    file_name: str
    title: str
    raw_text: str
    metadata: dict[str, Any] = field(default_factory=dict)
    ingested_at: str = field(default_factory=utc_now_iso)


@dataclass(slots=True)
class DocumentSection:
    """
    Parsed structural section of a document.
    Used later by the parsing stage.
    """
    section_id: str
    doc_id: str
    title: str
    level: int
    text: str
    parent_section_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ParsedDocument:
    """
    Output of parsing stage.
    Keeps structural information extracted from RawDocument.
    """
    doc_id: str
    corpus: str
    title: str
    source_type: str
    source_path: str
    sections: list[DocumentSection]
    metadata: dict[str, Any] = field(default_factory=dict)
    parsed_at: str = field(default_factory=utc_now_iso)


@dataclass(slots=True)
class DocumentChunk:
    """
    Output of chunking stage.
    Unit used for embedding and retrieval.
    """
    chunk_id: str
    doc_id: str
    corpus: str
    text: str
    chunk_index: int
    section_title: str | None = None
    section_path: list[str] = field(default_factory=list)
    page_number: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    chunked_at: str = field(default_factory=utc_now_iso)
# parsing/parsers.py

from __future__ import annotations

import hashlib
import re
from abc import ABC, abstractmethod

from schemas.documents import DocumentSection, ParsedDocument, RawDocument


FRONTMATTER_PATTERN = re.compile(r"^---\n.*?\n---\n?", re.DOTALL)
STYLE_BLOCK_PATTERN = re.compile(r"<style.*?>.*?</style>", re.DOTALL | re.IGNORECASE)
HTML_COMMENT_PATTERN = re.compile(r"<!--.*?-->", re.DOTALL)
MULTI_BLANK_LINES_PATTERN = re.compile(r"\n{3,}")
HEADING_PATTERN = re.compile(r"^(#{1,6})\s+(.+?)\s*$", re.MULTILINE)
HEADING_ANCHOR_SUFFIX_PATTERN = re.compile(r"\s*\{\s*#.*?\s*\}\s*$")


class BaseParser(ABC):
    supported_source_types: set[str] = set()

    def can_parse(self, document: RawDocument) -> bool:
        return document.source_type in self.supported_source_types

    @abstractmethod
    def parse(self, document: RawDocument) -> ParsedDocument:
        raise NotImplementedError


class MarkdownStructuredParser(BaseParser):
    supported_source_types = {"markdown"}

    def parse(self, document: RawDocument) -> ParsedDocument:
        cleaned_text = self._clean_markdown(document.raw_text)
        sections = self._extract_sections(
            doc_id=document.doc_id,
            fallback_title=document.title,
            text=cleaned_text,
        )

        return ParsedDocument(
            doc_id=document.doc_id,
            corpus=document.corpus,
            title=document.title,
            source_type=document.source_type,
            source_path=document.source_path,
            sections=sections,
            metadata={
                **document.metadata,
                "parser": "MarkdownStructuredParser",
                "section_count": len(sections),
            },
        )

    def _clean_markdown(self, text: str) -> str:
        cleaned = text.replace("\r\n", "\n")
        cleaned = FRONTMATTER_PATTERN.sub("", cleaned)
        cleaned = STYLE_BLOCK_PATTERN.sub("", cleaned)
        cleaned = HTML_COMMENT_PATTERN.sub("", cleaned)
        cleaned = MULTI_BLANK_LINES_PATTERN.sub("\n\n", cleaned)
        return cleaned.strip()

    def _extract_sections(
        self,
        doc_id: str,
        fallback_title: str,
        text: str,
    ) -> list[DocumentSection]:
        matches = list(HEADING_PATTERN.finditer(text))

        if not matches:
            fallback_text = text.strip()
            return [
                DocumentSection(
                    section_id=self._build_section_id(doc_id, [fallback_title], 0),
                    doc_id=doc_id,
                    title=fallback_title,
                    level=1,
                    text=fallback_text,
                    parent_section_id=None,
                    metadata={
                        "heading_path": [fallback_title],
                        "ordinal": 0,
                        "generated": True,
                    },
                )
            ]

        sections: list[DocumentSection] = []
        heading_stack: list[tuple[int, str, str]] = []
        intro_text = text[:matches[0].start()].strip()

        for ordinal, match in enumerate(matches):
            heading_level = len(match.group(1))
            raw_title = match.group(2)
            title = self._normalize_heading_title(raw_title)

            content_start = match.end()
            content_end = matches[ordinal + 1].start() if ordinal + 1 < len(matches) else len(text)
            section_text = text[content_start:content_end].strip()

            if ordinal == 0 and intro_text:
                section_text = f"{intro_text}\n\n{section_text}".strip()

            while heading_stack and heading_stack[-1][0] >= heading_level:
                heading_stack.pop()

            parent_section_id = heading_stack[-1][1] if heading_stack else None
            heading_path = [item[2] for item in heading_stack] + [title]
            section_id = self._build_section_id(doc_id, heading_path, ordinal)

            section = DocumentSection(
                section_id=section_id,
                doc_id=doc_id,
                title=title,
                level=heading_level,
                text=section_text,
                parent_section_id=parent_section_id,
                metadata={
                    "heading_path": heading_path,
                    "ordinal": ordinal,
                },
            )
            sections.append(section)
            heading_stack.append((heading_level, section_id, title))

        return sections

    @staticmethod
    def _normalize_heading_title(raw_title: str) -> str:
        title = raw_title.strip()
        title = HEADING_ANCHOR_SUFFIX_PATTERN.sub("", title)
        return title.strip()

    @staticmethod
    def _build_section_id(
        doc_id: str,
        heading_path: list[str],
        ordinal: int,
    ) -> str:
        raw_key = "::".join([doc_id, *heading_path, str(ordinal)])
        digest = hashlib.sha1(raw_key.encode("utf-8")).hexdigest()[:12]
        return f"{doc_id}_sec_{digest}"


class PlainTextParser(BaseParser):
    supported_source_types = {"text"}

    def parse(self, document: RawDocument) -> ParsedDocument:
        title = document.title.strip() or document.file_name
        text = document.raw_text.strip()

        section = DocumentSection(
            section_id=self._build_section_id(document.doc_id, [title], 0),
            doc_id=document.doc_id,
            title=title,
            level=1,
            text=text,
            parent_section_id=None,
            metadata={
                "heading_path": [title],
                "ordinal": 0,
                "generated": True,
            },
        )

        return ParsedDocument(
            doc_id=document.doc_id,
            corpus=document.corpus,
            title=document.title,
            source_type=document.source_type,
            source_path=document.source_path,
            sections=[section],
            metadata={
                **document.metadata,
                "parser": "PlainTextParser",
                "section_count": 1,
            },
        )

    @staticmethod
    def _build_section_id(
        doc_id: str,
        heading_path: list[str],
        ordinal: int,
    ) -> str:
        raw_key = "::".join([doc_id, *heading_path, str(ordinal)])
        digest = hashlib.sha1(raw_key.encode("utf-8")).hexdigest()[:12]
        return f"{doc_id}_sec_{digest}"


class ParserRegistry:
    def __init__(self) -> None:
        self._parsers = [
            MarkdownStructuredParser(),
            PlainTextParser(),
        ]

    def get_parser(self, document: RawDocument) -> BaseParser:
        for parser in self._parsers:
            if parser.can_parse(document):
                return parser
        raise ValueError(
            f"No parser found for source_type={document.source_type!r} "
            f"doc_id={document.doc_id!r}"
        )
# parsing/parsers.py

from __future__ import annotations

import hashlib
import re
from abc import ABC, abstractmethod
from typing import Any

from schemas.documents import DocumentSection, ParsedDocument, RawDocument


FRONTMATTER_PATTERN = re.compile(r"^---\n.*?\n---\n?", re.DOTALL)
STYLE_BLOCK_PATTERN = re.compile(r"<style.*?>.*?</style>", re.DOTALL | re.IGNORECASE)
HTML_COMMENT_PATTERN = re.compile(r"<!--.*?-->", re.DOTALL)
MULTI_BLANK_LINES_PATTERN = re.compile(r"\n{3,}")
HEADING_PATTERN = re.compile(r"^(#{1,6})\s+(.+?)\s*$", re.MULTILINE)
HEADING_ANCHOR_SUFFIX_PATTERN = re.compile(r"\s*\{\s*#.*?\s*\}\s*$")

SEC_ITEM_LINE_PATTERN = re.compile(
    r"^\s*(Item\s+\d+[A-Z]?)\.\s+(.+?)\s*$",
    re.IGNORECASE,
)

SEC_ITEM_HEADING_PATTERN = re.compile(
    r"^##\s+(Item\s+\d+[A-Z]?)\.\s+(.+?)\s*$",
    re.IGNORECASE,
)

PART_HEADING_PATTERN = re.compile(
    r"^##\s+(PART\s+[IVX]+)\s*$",
    re.IGNORECASE,
)

TABLE_OF_CONTENTS_PATTERN = re.compile(
    r"^##\s+TABLE OF CONTENTS\s*$",
    re.IGNORECASE,
)

MARKDOWN_TABLE_ROW_PATTERN = re.compile(r"^\s*\|.*\|\s*$")


ITEM_SECTION_TYPE_MAP = {
    "item 1": "business_overview",
    "item 1a": "risk_factors",
    "item 1b": "unresolved_staff_comments",
    "item 1c": "cybersecurity",
    "item 2": "properties",
    "item 3": "legal_proceedings",
    "item 4": "mine_safety_disclosures",
    "item 5": "market_information",
    "item 6": "reserved",
    "item 7": "management_discussion",
    "item 7a": "market_risk",
    "item 8": "financial_statements",
    "item 9": "accounting_disagreements",
    "item 9a": "controls_and_procedures",
    "item 9b": "other_information",
    "item 9c": "foreign_jurisdictions_disclosure",
    "item 10": "directors_and_governance",
    "item 11": "executive_compensation",
    "item 12": "security_ownership",
    "item 13": "related_transactions",
    "item 14": "accountant_fees",
    "item 15": "exhibits",
    "item 16": "form_10k_summary",
}


CRITICAL_ITEMS = {
    "item 1": "has_item_1_business",
    "item 1a": "has_item_1a_risk_factors",
    "item 7": "has_item_7_mda",
    "item 8": "has_item_8_financial_statements",
}


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


class FinancialTenKParser(BaseParser):
    """
    Parser for Docling-extracted SEC 10-K markdown.

    Main rule:
    - SEC Item headings become DocumentSection boundaries.
    - Non-SEC ## headings stay inside DocumentSection.text as subsection context.
    """

    supported_source_types = {"pdf"}

    def parse(self, document: RawDocument) -> ParsedDocument:
        cleaned_text = self._clean_text(document.raw_text)

        audit_log: dict[str, Any] = {
            "parser": "FinancialTenKParser",
            "file_name": document.file_name,
            "company": document.metadata.get("company"),
            "ticker": document.metadata.get("ticker"),
            "fiscal_year": document.metadata.get("fiscal_year"),
            "promoted_headings": [],
            "subsection_headings": [],
            "detected_items": [],
            "sections": [],
            "warnings": [],
        }

        normalized_text = self._promote_sec_item_headings(
            text=cleaned_text,
            audit_log=audit_log,
        )

        sections = self._extract_sec_item_sections(
            doc_id=document.doc_id,
            text=normalized_text,
            audit_log=audit_log,
        )

        validation = self._build_validation(sections)
        audit_log["section_count"] = len(sections)
        audit_log["promoted_heading_count"] = len(audit_log["promoted_headings"])
        audit_log["subsection_heading_count"] = len(audit_log["subsection_headings"])
        audit_log["validation"] = validation

        if not sections:
            audit_log["warnings"].append(
                "No SEC item sections detected. Falling back to full-document section."
            )
            sections = [
                DocumentSection(
                    section_id=self._build_section_id(
                        document.doc_id,
                        [document.title],
                        0,
                    ),
                    doc_id=document.doc_id,
                    title=document.title,
                    level=1,
                    text=normalized_text,
                    parent_section_id=None,
                    metadata={
                        "heading_path": [document.title],
                        "ordinal": 0,
                        "generated": True,
                        "section_type": "unknown",
                    },
                )
            ]

        return ParsedDocument(
            doc_id=document.doc_id,
            corpus=document.corpus,
            title=document.title,
            source_type=document.source_type,
            source_path=document.source_path,
            sections=sections,
            metadata={
                **document.metadata,
                "parser": "FinancialTenKParser",
                "section_count": len(sections),
                "parsing_audit": audit_log,
            },
        )

    @staticmethod
    def _clean_text(text: str) -> str:
        cleaned = text.replace("\r\n", "\n")
        cleaned = HTML_COMMENT_PATTERN.sub("", cleaned)
        cleaned = MULTI_BLANK_LINES_PATTERN.sub("\n\n", cleaned)
        return cleaned.strip()

    def _promote_sec_item_headings(
        self,
        text: str,
        audit_log: dict[str, Any],
    ) -> str:
        lines = text.splitlines()
        promoted_lines: list[str] = []

        inside_table_of_contents = False

        for line in lines:
            stripped = line.strip()

            if TABLE_OF_CONTENTS_PATTERN.match(stripped):
                inside_table_of_contents = True
                promoted_lines.append(line)
                continue

            if inside_table_of_contents:
                promoted_lines.append(line)

                # Most TOC lines are markdown table rows.
                # Once the table ends and a non-table heading starts, leave TOC mode.
                if stripped.startswith("##") and not TABLE_OF_CONTENTS_PATTERN.match(stripped):
                    inside_table_of_contents = False

                continue

            if stripped.startswith("#"):
                promoted_lines.append(line)
                continue

            match = SEC_ITEM_LINE_PATTERN.match(stripped)
            if match:
                item = self._normalize_item(match.group(1))
                title = self._normalize_spaces(match.group(2))
                promoted_heading = f"## {item}. {title}"

                promoted_lines.append(promoted_heading)

                audit_log["promoted_headings"].append(
                    {
                        "original_line": line,
                        "new_heading": promoted_heading,
                        "reason": "SEC item line detected without markdown heading marker",
                    }
                )
                continue

            promoted_lines.append(line)

        return "\n".join(promoted_lines).strip()

    def _extract_sec_item_sections(
        self,
        doc_id: str,
        text: str,
        audit_log: dict[str, Any],
    ) -> list[DocumentSection]:
        lines = text.splitlines()

        sections: list[DocumentSection] = []
        current_part: str | None = None
        current_section: dict[str, Any] | None = None
        ordinal = 0

        for line in lines:
            stripped = line.strip()

            part_match = PART_HEADING_PATTERN.match(stripped)
            if part_match:
                current_part = part_match.group(1).upper()
                if current_section is not None:
                    current_section["lines"].append(line)
                continue

            sec_item_match = SEC_ITEM_HEADING_PATTERN.match(stripped)

            if sec_item_match:
                if current_section is not None:
                    section = self._finalize_section(
                        doc_id=doc_id,
                        section_data=current_section,
                        ordinal=ordinal,
                    )
                    sections.append(section)
                    self._append_section_audit(section, audit_log)
                    ordinal += 1

                item = self._normalize_item(sec_item_match.group(1))
                title_text = self._normalize_spaces(sec_item_match.group(2))
                section_title = f"{item}. {title_text}"
                normalized_item_key = item.lower()

                current_section = {
                    "title": section_title,
                    "item": item,
                    "part": current_part,
                    "lines": [],
                    "subheadings": [],
                    "section_type": ITEM_SECTION_TYPE_MAP.get(
                        normalized_item_key,
                        "unknown",
                    ),
                }

                if item not in audit_log["detected_items"]:
                    audit_log["detected_items"].append(item)

                continue

            if current_section is None:
                continue

            non_sec_heading = self._extract_non_sec_heading(stripped)
            if non_sec_heading:
                current_section["subheadings"].append(non_sec_heading)
                audit_log["subsection_headings"].append(
                    {
                        "heading": non_sec_heading,
                        "assigned_to": current_section["title"],
                    }
                )

            current_section["lines"].append(line)

        if current_section is not None:
            section = self._finalize_section(
                doc_id=doc_id,
                section_data=current_section,
                ordinal=ordinal,
            )
            sections.append(section)
            self._append_section_audit(section, audit_log)

        return sections

    def _finalize_section(
        self,
        doc_id: str,
        section_data: dict[str, Any],
        ordinal: int,
    ) -> DocumentSection:
        title = section_data["title"]
        part = section_data.get("part")
        item = section_data["item"]
        section_type = section_data["section_type"]
        subheadings = section_data["subheadings"]
        text = "\n".join(section_data["lines"]).strip()

        heading_path = []
        if part:
            heading_path.append(part)
        heading_path.append(title)

        section_id = self._build_section_id(doc_id, heading_path, ordinal)

        return DocumentSection(
            section_id=section_id,
            doc_id=doc_id,
            title=title,
            level=2,
            text=text,
            parent_section_id=None,
            metadata={
                "heading_path": heading_path,
                "ordinal": ordinal,
                "part": part,
                "form_item": item,
                "section_type": section_type,
                "subheadings": subheadings,
                "subheading_count": len(subheadings),
            },
        )

    @staticmethod
    def _append_section_audit(
        section: DocumentSection,
        audit_log: dict[str, Any],
    ) -> None:
        audit_log["sections"].append(
            {
                "section_id": section.section_id,
                "title": section.title,
                "form_item": section.metadata.get("form_item"),
                "section_type": section.metadata.get("section_type"),
                "part": section.metadata.get("part"),
                "text_length": len(section.text),
                "subheading_count": section.metadata.get("subheading_count", 0),
                "subheadings_preview": section.metadata.get("subheadings", [])[:10],
            }
        )

    @staticmethod
    def _extract_non_sec_heading(line: str) -> str | None:
        if not line.startswith("##"):
            return None

        if SEC_ITEM_HEADING_PATTERN.match(line):
            return None

        if PART_HEADING_PATTERN.match(line):
            return None

        if TABLE_OF_CONTENTS_PATTERN.match(line):
            return None

        title = line.lstrip("#").strip()
        return title or None

    @staticmethod
    def _build_validation(sections: list[DocumentSection]) -> dict[str, Any]:
        found_items = {
            (section.metadata.get("form_item") or "").lower()
            for section in sections
        }

        validation = {
            key: item in found_items
            for item, key in CRITICAL_ITEMS.items()
        }

        validation["has_empty_critical_sections"] = any(
            (section.metadata.get("form_item") or "").lower() in CRITICAL_ITEMS
            and not section.text.strip()
            for section in sections
        )

        validation["passed"] = (
            validation["has_item_1_business"]
            and validation["has_item_1a_risk_factors"]
            and validation["has_item_7_mda"]
            and validation["has_item_8_financial_statements"]
            and not validation["has_empty_critical_sections"]
        )

        return validation

    @staticmethod
    def _normalize_item(raw_item: str) -> str:
        normalized = " ".join(raw_item.strip().split())
        normalized = normalized.replace("ITEM", "Item").replace("item", "Item")
        return normalized

    @staticmethod
    def _normalize_spaces(text: str) -> str:
        return " ".join(text.strip().split())

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
            FinancialTenKParser(),
        ]

    def get_parser(self, document: RawDocument) -> BaseParser:
        for parser in self._parsers:
            if parser.can_parse(document):
                return parser
        raise ValueError(
            f"No parser found for source_type={document.source_type!r} "
            f"doc_id={document.doc_id!r}"
        )
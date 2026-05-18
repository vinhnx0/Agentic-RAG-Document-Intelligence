# ingestion/loaders.py

from __future__ import annotations

import hashlib
import re
from abc import ABC, abstractmethod
from pathlib import Path

from schemas.documents import RawDocument


FIRST_HEADING_PATTERN = re.compile(r"^\s*#\s+(.+?)\s*$", re.MULTILINE)
YEAR_PATTERN = re.compile(r"(20\d{2})")


COMPANY_TICKER_MAP = {
    "apple": ("Apple", "AAPL"),
    "aapl": ("Apple", "AAPL"),
    "microsoft": ("Microsoft", "MSFT"),
    "msft": ("Microsoft", "MSFT"),
    "amazon": ("Amazon", "AMZN"),
    "amzn": ("Amazon", "AMZN"),
}


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
        content_hash = self._build_content_hash(raw_text)
        doc_id = self._build_doc_id(corpus_name, content_hash)

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
                "content_hash": content_hash,
                "original_source_path": str(file_path),
            },
        )

    @staticmethod
    def _extract_title(raw_text: str, file_path: Path) -> str:
        match = FIRST_HEADING_PATTERN.search(raw_text)
        if match:
            return match.group(1).strip()
        return file_path.stem.replace("_", " ").replace("-", " ").strip()

    @staticmethod
    def _build_content_hash(raw_text: str) -> str:
        normalized_text = raw_text.replace("\r\n", "\n").strip()
        return hashlib.sha1(normalized_text.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def _build_doc_id(corpus_name: str, content_hash: str) -> str:
        return f"{corpus_name}_{content_hash}"


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
        content_hash = self._build_content_hash(raw_text)
        doc_id = self._build_doc_id(corpus_name, content_hash)

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
                "content_hash": content_hash,
                "original_source_path": str(file_path),
            },
        )

    @staticmethod
    def _build_content_hash(raw_text: str) -> str:
        normalized_text = raw_text.replace("\r\n", "\n").strip()
        return hashlib.sha1(normalized_text.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def _build_doc_id(corpus_name: str, content_hash: str) -> str:
        return f"{corpus_name}_{content_hash}"


class DoclingPDFLoader(BaseLoader):
    """
    PDF loader for Dataset B financial reports.

    Uses Docling to convert complex PDFs into markdown-like structured text.
    The output still becomes RawDocument, so the rest of your pipeline remains unchanged.
    """

    supported_extensions = {"pdf"}

    def load(
        self,
        file_path: Path,
        corpus_name: str,
        encoding: str = "utf-8",
    ) -> RawDocument:
        raw_text = self._load_or_create_cache(file_path=file_path)

        if not raw_text.strip():
            raise ValueError(f"Docling extracted empty text from: {file_path}")

        content_hash = self._build_content_hash(raw_text)
        doc_id = self._build_doc_id(corpus_name, content_hash)

        company, ticker, fiscal_year = self._extract_financial_metadata(file_path)
        title = self._build_title(
            company=company,
            fiscal_year=fiscal_year,
            file_path=file_path,
        )

        return RawDocument(
            doc_id=doc_id,
            corpus=corpus_name,
            source_type="pdf",
            source_path=str(file_path),
            file_name=file_path.name,
            title=title,
            raw_text=raw_text,
            metadata={
                "extension": file_path.suffix.lower(),
                "loader": "DoclingPDFLoader",
                "content_hash": content_hash,
                "original_source_path": str(file_path),
                "parser_backend": "docling",
                "company": company,
                "ticker": ticker,
                "fiscal_year": fiscal_year,
                "report_type": "annual_report",
            },
        )

    def _load_or_create_cache(self, file_path: Path) -> str:
        cache_path = self._get_cache_path(file_path)

        if cache_path.exists():
            return cache_path.read_text(encoding="utf-8")

        raw_text = self._convert_pdf_to_markdown(file_path)

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(raw_text, encoding="utf-8")

        return raw_text

    @staticmethod
    def _convert_pdf_to_markdown(file_path: Path) -> str:
        import os

        # Reduce CPU thread pressure.
        # Docling docs default CPU threads is 4; fewer threads can reduce resource usage.
        os.environ.setdefault("OMP_NUM_THREADS", "1")
        os.environ.setdefault("DOCLING_NUM_THREADS", "1")

        try:
            from docling.datamodel.base_models import InputFormat
            from docling.datamodel.pipeline_options import (
                PdfPipelineOptions,
                AcceleratorOptions,
            )
            from docling.document_converter import DocumentConverter, PdfFormatOption
        except ImportError as exc:
            raise ImportError(
                "Docling is not installed. Install it with: pip install docling"
            ) from exc

        pipeline_options = PdfPipelineOptions()

        # Disable OCR to avoid unnecessary image-based processing.
        pipeline_options.do_ocr = False

        # Disable table structure detection
        pipeline_options.do_table_structure = True

        # Important low-memory option:
        # use backend/native PDF text instead of layout-model text detection.
        pipeline_options.force_backend_text = True

        # Disable optional image outputs / intermediate outputs.
        pipeline_options.generate_page_images = False
        pipeline_options.generate_picture_images = False
        pipeline_options.generate_table_images = False
        pipeline_options.generate_parsed_pages = False

        # Disable optional enrichment.
        pipeline_options.do_picture_classification = False
        pipeline_options.do_picture_description = False
        pipeline_options.do_code_enrichment = False
        pipeline_options.do_formula_enrichment = False
        pipeline_options.do_chart_extraction = False

        # Keep image scale low/default.
        # Higher values increase processing time/storage.
        pipeline_options.images_scale = 1.0

        # Reduce memory pressure from batching/queues.
        pipeline_options.ocr_batch_size = 1
        pipeline_options.layout_batch_size = 1
        pipeline_options.table_batch_size = 1
        pipeline_options.queue_max_size = 1

        # CPU only, fewer threads.
        pipeline_options.accelerator_options = AcceleratorOptions(
            num_threads=1,
            device="cpu",
        )

        converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(
                    pipeline_options=pipeline_options
                )
            }
        )

        result = converter.convert(str(file_path))

        markdown_text = result.document.export_to_markdown()
        return markdown_text.strip()

    @staticmethod
    def _get_cache_path(file_path: Path) -> Path:
        """
        Cache Docling output so the same PDF is not parsed repeatedly.

        Example:
        data/raw/financial_reports/apple_2023.pdf
        ->
        data/processed/financial_reports/docling_cache/apple_2023.md
        """
        return (
            Path("data")
            / "processed"
            / "financial_reports"
            / "docling_cache"
            / f"{file_path.stem}.md"
        )

    @staticmethod
    def _extract_financial_metadata(file_path: Path) -> tuple[str, str | None, int | None]:
        """
        Simple filename-based metadata extraction.

        Expected filename examples:
        - apple_2023.pdf
        - microsoft_2024.pdf
        - nvidia_2022.pdf
        """
        stem = file_path.stem.lower()
        parts = re.split(r"[_\-\s]+", stem)

        company_key = parts[0] if parts else stem

        company_ticker = COMPANY_TICKER_MAP.get(company_key)

        if company_ticker:
            company, ticker = company_ticker
        else:
            company = company_key.title()
            ticker = None

        year_match = YEAR_PATTERN.search(stem)
        fiscal_year = int(year_match.group(1)) if year_match else None

        return company, ticker, fiscal_year

    @staticmethod
    def _build_title(
        company: str,
        fiscal_year: int | None,
        file_path: Path,
    ) -> str:
        if company and fiscal_year:
            return f"{company} {fiscal_year} Annual Report"
        return file_path.stem.replace("_", " ").replace("-", " ").strip().title()

    @staticmethod
    def _build_content_hash(raw_text: str) -> str:
        normalized_text = raw_text.replace("\r\n", "\n").strip()
        return hashlib.sha1(normalized_text.encode("utf-8")).hexdigest()[:16]

    @staticmethod
    def _build_doc_id(corpus_name: str, content_hash: str) -> str:
        return f"{corpus_name}_{content_hash}"


class LoaderRegistry:
    def __init__(self) -> None:
        self._loaders = [
            MarkdownLoader(),
            TextLoader(),
            DoclingPDFLoader(),
        ]

    def get_loader(self, file_path: Path) -> BaseLoader:
        for loader in self._loaders:
            if loader.can_load(file_path):
                return loader
        raise ValueError(f"No loader found for file: {file_path}")
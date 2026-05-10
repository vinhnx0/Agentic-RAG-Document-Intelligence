# rag/generators.py

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseAnswerGenerator(ABC):
    @abstractmethod
    def generate(
        self,
        query: str,
        context_chunks: list[dict[str, Any]],
    ) -> str:
        raise NotImplementedError


class ExtractiveAnswerGenerator(BaseAnswerGenerator):
    """
    First production-safe answer generator.

    It does not call an external LLM yet.
    It builds an answer from reranked chunks and attaches citation markers.
    This is useful for testing context quality and citation correctness.
    """

    def __init__(self, max_chars_per_chunk: int = 1200) -> None:
        if max_chars_per_chunk <= 0:
            raise ValueError("max_chars_per_chunk must be > 0")

        self.max_chars_per_chunk = max_chars_per_chunk

    def generate(
        self,
        query: str,
        context_chunks: list[dict[str, Any]],
    ) -> str:
        clean_query = query.strip()
        if not clean_query:
            raise ValueError("query must not be empty")

        if not context_chunks:
            return (
                "I could not find enough relevant context to answer this question."
            )

        answer_parts = [
            f"Based on the retrieved documentation, here is the most relevant information for: {clean_query}\n"
        ]

        for index, chunk in enumerate(context_chunks, start=1):
            section_title = chunk.get("section_title") or "Untitled section"
            text = chunk.get("text") or ""
            text = self._truncate_text(text)

            answer_parts.append(
                f"[{index}] {section_title}\n{text}"
            )

        return "\n\n".join(answer_parts)

    def _truncate_text(self, text: str) -> str:
        cleaned = " ".join(text.split())

        if len(cleaned) <= self.max_chars_per_chunk:
            return cleaned

        return cleaned[: self.max_chars_per_chunk].rstrip() + "..."


class AnswerGeneratorRegistry:
    def __init__(
        self,
        generator_type: str,
        max_chars_per_chunk: int,
    ) -> None:
        if generator_type != "extractive":
            raise ValueError(f"Unsupported answer generator: {generator_type}")

        self._generator = ExtractiveAnswerGenerator(
            max_chars_per_chunk=max_chars_per_chunk,
        )

    def get_generator(self) -> BaseAnswerGenerator:
        return self._generator
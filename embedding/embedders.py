# embedding/embedders.py

from __future__ import annotations

from abc import ABC, abstractmethod

from sentence_transformers import SentenceTransformer


class BaseEmbedder(ABC):
    @abstractmethod
    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        raise NotImplementedError

    @abstractmethod
    def get_model_name(self) -> str:
        raise NotImplementedError


class SentenceTransformerEmbedder(BaseEmbedder):
    def __init__(
        self,
        model_name: str,
        batch_size: int = 32,
        normalize_embeddings: bool = True,
    ) -> None:
        if not model_name:
            raise ValueError("model_name must not be empty")
        if batch_size <= 0:
            raise ValueError("batch_size must be > 0")

        self.model_name = model_name
        self.batch_size = batch_size
        self.normalize_embeddings = normalize_embeddings
        self.model = SentenceTransformer(model_name)

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        embeddings = self.model.encode(
            texts,
            batch_size=self.batch_size,
            normalize_embeddings=self.normalize_embeddings,
            convert_to_numpy=True,
            show_progress_bar=True,
        )

        return embeddings.astype(float).tolist()

    def get_model_name(self) -> str:
        return self.model_name


class EmbedderRegistry:
    def __init__(
        self,
        model_name: str,
        batch_size: int = 32,
        normalize_embeddings: bool = True,
    ) -> None:
        self._embedder = SentenceTransformerEmbedder(
            model_name=model_name,
            batch_size=batch_size,
            normalize_embeddings=normalize_embeddings,
        )

    def get_embedder(self) -> BaseEmbedder:
        return self._embedder
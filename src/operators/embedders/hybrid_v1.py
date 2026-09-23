"""`hybrid@1`: dense and sparse generation, query encoding, and how results are fused.

These are the operator's internals, not separate pipeline stages. The orchestrator asks
for `hybrid@1` and gets embeddings; the store ranks them with the settings this operator
hands it. The version is immutable: `CONFIG` is part of what `hybrid@1` means, so a
different model is a new embedder version, never a different configuration of this one.
"""

import os
from functools import cache
from typing import Any, ClassVar

import onnxruntime as ort
from fastembed import SparseTextEmbedding, TextEmbedding

from contracts.embeddings import (
    PGVECTOR_MAX_SPARSE_DIMENSIONS,
    Embedding,
    SearchOptions,
    SparseVector,
)
from contracts.operators import OperatorRef


def configured_device() -> str:
    return os.getenv("EMBEDDING_DEVICE", "auto").lower()


def available_embedding_providers(device: str = "auto") -> tuple[str, ...]:
    """Select CUDA when requested and available, with a CPU fallback."""
    available = set(ort.get_available_providers())
    if device not in {"auto", "cpu", "cuda"}:
        raise ValueError("embedding device must be auto, cpu, or cuda")
    if device == "cuda" and "CUDAExecutionProvider" not in available:
        raise RuntimeError("CUDA was requested but its ONNX provider is unavailable")
    if device != "cpu" and "CUDAExecutionProvider" in available:
        return ("CUDAExecutionProvider", "CPUExecutionProvider")
    return ("CPUExecutionProvider",)


@cache
def _dense_model(model_name: str, providers: tuple[str, ...]):
    return TextEmbedding(model_name=model_name, providers=list(providers))


@cache
def _sparse_model(model_name: str, providers: tuple[str, ...]):
    return SparseTextEmbedding(model_name=model_name, providers=list(providers))


def sparse_dimension(model_name: str) -> int:
    for model in SparseTextEmbedding.list_supported_models():
        if model["model"].lower() == model_name.lower():
            # BM25/BM42 return 31-bit MurmurHash IDs rather than vocabulary IDs.
            # pgvector supports sparse vectors with at most one billion dimensions.
            if model.get("requires_idf"):
                return PGVECTOR_MAX_SPARSE_DIMENSIONS
            dimension = model.get("vocab_size")
            if dimension:
                return int(dimension)
            break
    raise ValueError(f"cannot determine sparse vector dimension for {model_name}")


def normalise_sparse_vector(vector, dimension: int) -> SparseVector:
    """Fit FastEmbed indices into pgvector and merge rare hash collisions."""
    values_by_index: dict[int, float] = {}
    for raw_index, raw_value in zip(vector.indices, vector.values, strict=True):
        index = int(raw_index) % dimension
        values_by_index[index] = values_by_index.get(index, 0.0) + float(raw_value)

    return SparseVector(
        indices=list(values_by_index),
        values=list(values_by_index.values()),
        dimension=dimension,
    )


class HybridV1:
    ref: ClassVar[OperatorRef] = OperatorRef(name="hybrid", version="1")

    # The execution device is deliberately absent: running on CUDA or CPU must not
    # produce a different artifact.
    CONFIG: ClassVar[dict[str, Any]] = {
        "dense_model": "BAAI/bge-small-en-v1.5",
        "sparse_model": "Qdrant/bm42-all-minilm-l6-v2-attentions",
        "fusion": {"method": "reciprocal-rank", "k": 60},
        "candidates": {"multiplier": 5, "minimum": 50},
    }

    def __init__(self, device: str | None = None) -> None:
        self.device = device or configured_device()

    def embed(self, texts: list[str]) -> list[Embedding]:
        """Embed both ways, in the order the texts were given."""
        providers = available_embedding_providers(self.device)
        sparse_name = self.CONFIG["sparse_model"]
        dimension = sparse_dimension(sparse_name)
        dense = _dense_model(self.CONFIG["dense_model"], providers).embed(texts)
        sparse = _sparse_model(sparse_name, providers).embed(texts)
        return [
            Embedding(
                dense=dense_vector.tolist(),
                sparse=normalise_sparse_vector(sparse_vector, dimension),
            )
            for dense_vector, sparse_vector in zip(dense, sparse, strict=True)
        ]

    def embed_query(self, text: str) -> Embedding:
        """Embed a query with each model's query-specific path."""
        providers = available_embedding_providers(self.device)
        sparse_name = self.CONFIG["sparse_model"]
        dense = next(_dense_model(self.CONFIG["dense_model"], providers).query_embed(text))
        sparse = next(_sparse_model(sparse_name, providers).query_embed(text))
        return Embedding(
            dense=dense.tolist(),
            sparse=normalise_sparse_vector(sparse, sparse_dimension(sparse_name)),
        )

    def search_options(self, limit: int = 3) -> SearchOptions:
        candidates = self.CONFIG["candidates"]
        return SearchOptions(
            embedder=self.ref,
            limit=limit,
            candidates=max(limit * candidates["multiplier"], candidates["minimum"]),
            fusion_k=int(self.CONFIG["fusion"]["k"]),
        )

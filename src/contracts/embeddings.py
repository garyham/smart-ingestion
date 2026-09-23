"""The embedding shapes an embedder hands to the store, and what a search asks and returns.

An embedder produces `Embedding`s and knows how its own results are ranked, which it
states as `SearchOptions`. The store keeps the embeddings and runs the search; it never
loads a model.
"""

from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from contracts.chunks import Chunk
from contracts.operators import OperatorRef

# pgvector's ceiling on a sparse vector's dimension.
PGVECTOR_MAX_SPARSE_DIMENSIONS = 1_000_000_000


class SparseVector(BaseModel):
    """Zero-based indices into a vector of `dimension` entries, and their values."""

    model_config = ConfigDict(frozen=True)

    indices: list[int]
    values: list[float]
    dimension: int = Field(ge=1, le=PGVECTOR_MAX_SPARSE_DIMENSIONS)

    @model_validator(mode="after")
    def _fits(self) -> Self:
        if len(self.indices) != len(self.values):
            raise ValueError("sparse indices and values have different lengths")
        if any(index < 0 or index >= self.dimension for index in self.indices):
            raise ValueError("sparse index is outside the declared dimension")
        return self


class Embedding(BaseModel):
    """One text, embedded both ways."""

    model_config = ConfigDict(frozen=True)

    dense: list[float]
    sparse: SparseVector


class SearchOptions(BaseModel):
    """Which embedder's embeddings to search, and how that embedder ranks them.

    `embedder` has no default: a search always names one version and never sweeps every
    embedding there is. `candidates` and `fusion_k` are part of what the embedder version
    means, so they come from `Embedder.search_options()` rather than from the caller.
    """

    model_config = ConfigDict(frozen=True)

    embedder: OperatorRef
    limit: int = Field(default=3, ge=1)
    candidates: int = Field(ge=1)
    fusion_k: int = Field(ge=1)


class SearchHit(BaseModel):
    """One chunk a search found, the document it came from, and how well it matched."""

    model_config = ConfigDict(frozen=True)

    chunk: Chunk
    document_name: str
    score: float

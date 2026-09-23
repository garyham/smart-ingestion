"""The chunk shapes a chunker hands to the store and the store hands back.

A chunker produces `ChunkDraft`s: text and its position, with no identity. The store gives
each one an ID when it keeps it, and from then on it is a `Chunk`.
"""

from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ChunkDraft(BaseModel):
    """A chunk before the store has kept it: a position in one document's text."""

    model_config = ConfigDict(frozen=True)

    index: int = Field(ge=0)
    headings: list[str] = Field(default_factory=list)
    text: str


class Chunk(BaseModel):
    """One chunk: a position in the text one extractor version got from one document,
    as one chunker version cut it."""

    model_config = ConfigDict(frozen=True)

    chunk_id: UUID
    extraction_id: UUID
    document_id: UUID
    extractor: str
    chunker: str
    index: int = Field(ge=0)
    headings: list[str] = Field(default_factory=list)
    text: str

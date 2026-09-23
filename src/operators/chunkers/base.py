"""What every chunker version is: a stateless class the flow calls before the store.

A chunker holds no connection and writes nothing. It turns text into `ChunkDraft`s and
nothing else - the text comes from an extractor, and keeping the chunks is the store's job.
"""

from typing import ClassVar, Protocol

from contracts.chunks import ChunkDraft
from contracts.operators import OperatorRef


class Chunker(Protocol):
    ref: ClassVar[OperatorRef]

    def split(self, text: str) -> list[ChunkDraft]: ...

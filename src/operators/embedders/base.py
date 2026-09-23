"""What every embedder version is: a stateless class the flow calls before the store.

An embedder holds models, not connections. It turns texts into `Embedding`s and states how
its own results are ranked; keeping and searching the embeddings is the store's job.
"""

from typing import ClassVar, Protocol

from contracts.embeddings import Embedding, SearchOptions
from contracts.operators import OperatorRef


class Embedder(Protocol):
    ref: ClassVar[OperatorRef]

    def embed(self, texts: list[str]) -> list[Embedding]: ...

    def embed_query(self, text: str) -> Embedding: ...

    def search_options(self, limit: int = 3) -> SearchOptions: ...

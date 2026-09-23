"""Chunker versions, looked up by `name@version`."""

from contracts.operators import OperatorRef
from operators.chunkers.base import Chunker
from operators.chunkers.chunker_v2 import ChunkerV2

_CHUNKERS: dict[str, type[Chunker]] = {str(ChunkerV2.ref): ChunkerV2}


class UnknownChunker(LookupError):
    pass


def get(chunker: str | OperatorRef) -> Chunker:
    operator = OperatorRef.parse(chunker)
    found = _CHUNKERS.get(str(operator))
    if found is None:
        raise UnknownChunker(f"no chunker {operator}")
    return found()

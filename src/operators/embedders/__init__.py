"""Embedder versions, looked up by `name@version`."""

from contracts.operators import OperatorRef
from operators.embedders.base import Embedder
from operators.embedders.hybrid_v1 import HybridV1

_EMBEDDERS: dict[str, type] = {str(HybridV1.ref): HybridV1}


class UnknownEmbedder(LookupError):
    pass


def get(embedder: str | OperatorRef, device: str | None = None) -> Embedder:
    """The embedder, ready to run on `device` (the configured one when not given).

    Models load on first use and are cached per process, so this is cheap to call.
    """
    operator = OperatorRef.parse(embedder)
    found = _EMBEDDERS.get(str(operator))
    if found is None:
        raise UnknownEmbedder(f"no embedder {operator}")
    return found(device)

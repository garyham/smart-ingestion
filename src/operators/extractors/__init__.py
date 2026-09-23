"""Extractor versions, looked up by `name@version`."""

from contracts.operators import OperatorRef
from operators.extractors.base import Extractor
from operators.extractors.tika_v1 import TikaV1

_EXTRACTORS: dict[str, type[Extractor]] = {str(TikaV1.ref): TikaV1}


class UnknownExtractor(LookupError):
    pass


def get(extractor: str | OperatorRef) -> Extractor:
    operator = OperatorRef.parse(extractor)
    found = _EXTRACTORS.get(str(operator))
    if found is None:
        raise UnknownExtractor(f"no extractor {operator}")
    return found()

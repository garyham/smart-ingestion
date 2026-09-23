"""What every extractor version is: a stateless class the flow calls before anything else.

An extractor holds no connection and writes nothing. It turns a document's bytes into its
MIME type, its plain text, and its metadata; every later stage works from that.
"""

from typing import ClassVar, Protocol

from contracts.extractions import Extraction
from contracts.operators import OperatorRef


class Extractor(Protocol):
    ref: ClassVar[OperatorRef]

    def extract(self, data: bytes, filename: str) -> Extraction: ...

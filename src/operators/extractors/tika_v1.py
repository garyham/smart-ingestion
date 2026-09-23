"""`tika@1`: one Apache Tika `rmeta/text` parse for the MIME type, text, and metadata.

The version is immutable: `CONFIG` is part of what `tika@1` means, so changing it is a new
extractor version. Chunks record the extractor whose text they were cut from.
"""

from typing import Any, ClassVar

from contracts.extractions import Extraction
from contracts.operators import OperatorRef
from ingestion.tika import document_metadata, parse_document


class TikaV1:
    ref: ClassVar[OperatorRef] = OperatorRef(name="tika", version="1")

    CONFIG: ClassVar[dict[str, Any]] = {"endpoint": "rmeta/text"}

    def extract(self, data: bytes, filename: str) -> Extraction:
        """Parse the bytes with Tika. The one network call an extractor makes."""
        tika = parse_document(data, filename)
        return Extraction(
            mime_type=tika.mime_type,
            text=tika.text,
            metadata={
                **document_metadata(filename, tika),
                "character_count": len(tika.text),
            },
        )

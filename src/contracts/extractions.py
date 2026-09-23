"""What an extractor produces: a document's MIME type, plain text, and metadata.

The store keeps it under the extractor version that produced it and hands back an
`ExtractionRef`, so the text never travels as a flow parameter.
"""

from typing import Any

from pydantic import BaseModel


class Extraction(BaseModel):
    """What an extractor got out of one document."""

    mime_type: str
    text: str
    metadata: dict[str, Any]

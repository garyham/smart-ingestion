import mimetypes
from dataclasses import dataclass
from pathlib import Path

import filetype
from prefect import task


@dataclass
class DetectedType:
    mime_type: str
    from_content: bool  # False when we had to fall back to the extension hint


@task
def identify_mime_type(path: Path) -> DetectedType:
    """Identify a document's MIME type from its content; the extension is only a fallback hint."""
    mime = filetype.guess_mime(str(path))
    from_content = mime is not None
    if mime is None:
        mime, _ = mimetypes.guess_type(path.name)
        mime = mime or "application/octet-stream"
    return DetectedType(mime_type=mime, from_content=from_content)

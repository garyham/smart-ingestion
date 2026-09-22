import os
from dataclasses import dataclass
from pathlib import Path

import httpx
from prefect import task

TIKA_URL = os.getenv("TIKA_URL", "http://127.0.0.1:9998")
TIKA_TIMEOUT_SECONDS = float(os.getenv("TIKA_TIMEOUT_SECONDS", "120"))


@dataclass
class TikaDocument:
    mime_type: str
    metadata: dict
    text: str


def _first(value, default=""):
    if isinstance(value, list):
        return value[0] if value else default
    return value if value is not None else default


def _content(record: dict) -> str:
    """Read the content key used by Tika 4 or older Tika releases."""
    value = record.get("tk:content", record.get("X-TIKA:content"))
    return str(_first(value))


@task
def extract_with_tika(path: Path) -> TikaDocument:
    """Detect the MIME type and extract metadata and plain text in one Tika parse."""
    response = httpx.put(
        f"{TIKA_URL.rstrip('/')}/rmeta/text",
        content=path.read_bytes(),
        headers={
            "Accept": "application/json",
            "Content-Type": "application/octet-stream",
            "Content-Disposition": f'attachment; filename="{path.name}"',
        },
        timeout=TIKA_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    result = response.json()

    records = result if isinstance(result, list) else [result]
    if not records or not isinstance(records[0], dict):
        raise ValueError("Tika returned no document metadata")

    primary = records[0]
    content_type = str(_first(primary.get("Content-Type"), "application/octet-stream"))
    mime_type = content_type.split(";", 1)[0].strip().lower()
    text = _content(primary)
    content_keys = {"tk:content", "X-TIKA:content"}
    metadata = {key: value for key, value in primary.items() if key not in content_keys}

    if len(records) > 1:
        metadata["embedded"] = [
            {key: value for key, value in record.items() if key not in content_keys}
            for record in records[1:]
            if isinstance(record, dict)
        ]

    return TikaDocument(mime_type=mime_type, metadata=metadata, text=text)


def document_metadata(doc: Path, tika: TikaDocument) -> dict:
    """Build the common metadata stored for every document."""
    title = _first(tika.metadata.get("dc:title")) or _first(tika.metadata.get("title"))
    return {
        "title": title or doc.stem,
        "mime_type": tika.mime_type,
        "origin_filename": doc.name,
        "converter": "apache-tika",
        "tika": tika.metadata,
    }

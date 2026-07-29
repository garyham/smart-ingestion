import json
from pathlib import Path

from langchain_text_splitters import MarkdownHeaderTextSplitter, RecursiveCharacterTextSplitter
from prefect import task

from ingestion.assets import write_status

_HEADERS_TO_SPLIT_ON = [("#", "h1"), ("##", "h2"), ("###", "h3"), ("####", "h4")]
_CHUNK_SIZE = 1000
_CHUNK_OVERLAP = 200

_header_splitter = MarkdownHeaderTextSplitter(headers_to_split_on=_HEADERS_TO_SPLIT_ON, strip_headers=True)
_size_splitter = RecursiveCharacterTextSplitter(chunk_size=_CHUNK_SIZE, chunk_overlap=_CHUNK_OVERLAP)


@task
def chunk_and_finalize(doc: Path, output_root: Path, markdown: str | None) -> None:
    """Split a converted document's markdown via langchain_text_splitters and write chunks.jsonl
    plus the final status. If `markdown` is None, the conversion step already failed and wrote
    its own status, so this is a no-op.
    """
    if markdown is None:
        return

    doc_dir = output_root / doc.stem

    if not markdown.strip():
        write_status(doc_dir, {"status": "needs_intervention", "reason": "no_content_extracted"})
        return

    chunks = []
    for section in _header_splitter.split_text(markdown):
        headings = [v for v in section.metadata.values() if v]
        for piece in _size_splitter.split_text(section.page_content):
            chunks.append({"index": len(chunks), "headings": headings, "text": piece})

    (doc_dir / "chunks.jsonl").write_text("\n".join(json.dumps(c) for c in chunks))
    write_status(doc_dir, {"status": "ok"})

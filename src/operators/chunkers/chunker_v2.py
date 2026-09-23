"""`chunker@2`: langchain_text_splitters, header-first and then by size. Text in, chunks out.

The version is immutable: `CONFIG` is part of what `chunker@2` means, so changing it is a
new chunker version. It never sees bytes - `chunker@1` also did Tika extraction, which is
now the extractor's job, so the text it splits is named by the extractor version as well.
"""

from typing import Any, ClassVar

from langchain_text_splitters import (
    MarkdownHeaderTextSplitter,
    RecursiveCharacterTextSplitter,
)

from contracts.chunks import ChunkDraft
from contracts.operators import OperatorRef

_HEADERS_TO_SPLIT_ON = [("#", "h1"), ("##", "h2"), ("###", "h3"), ("####", "h4")]


class ChunkerV2:
    ref: ClassVar[OperatorRef] = OperatorRef(name="chunker", version="2")

    CONFIG: ClassVar[dict[str, Any]] = {
        "splitter": "langchain.markdown_header+recursive",
        "chunk_size": 1000,
        "chunk_overlap": 200,
        "headers": [prefix for prefix, _ in _HEADERS_TO_SPLIT_ON],
        "strip_headers": True,
    }

    def split(self, text: str) -> list[ChunkDraft]:
        """Split text header-first, then cap each section by size. Pure."""
        header_splitter = MarkdownHeaderTextSplitter(
            headers_to_split_on=_HEADERS_TO_SPLIT_ON,
            strip_headers=self.CONFIG["strip_headers"],
        )
        size_splitter = RecursiveCharacterTextSplitter(
            chunk_size=self.CONFIG["chunk_size"],
            chunk_overlap=self.CONFIG["chunk_overlap"],
        )

        chunks: list[ChunkDraft] = []
        for section in header_splitter.split_text(text):
            headings = [value for value in section.metadata.values() if value]
            for piece in size_splitter.split_text(section.page_content):
                chunks.append(
                    ChunkDraft(index=len(chunks), headings=headings, text=piece)
                )
        return chunks

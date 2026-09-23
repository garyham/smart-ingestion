import unittest
from datetime import UTC, datetime
from uuid import UUID, uuid4

from contracts.chunks import Chunk, ChunkDraft
from contracts.operators import OperatorRef
from contracts.refs import DocumentStatus, EmbeddingsRef
from store.service import DocumentInfo

DOCUMENT_ID = UUID("c63752f4-8d18-4da2-a107-24c52d0707cc")
EXTRACTION_ID = UUID("0d6f4c1e-7b2a-4f3e-9c8d-5a1b2c3d4e5f")
CONTENT_ID = "a" * 64


class OperatorTests(unittest.TestCase):
    def test_parses_name_and_version(self):
        operator = OperatorRef.parse("chunker@1")
        self.assertEqual(operator.name, "chunker")
        self.assertEqual(operator.version, "1")
        self.assertEqual(str(operator), "chunker@1")

    def test_rejects_an_unversioned_operator(self):
        with self.assertRaises(ValueError):
            OperatorRef.parse("chunker")

    def test_parsing_an_operator_reference_is_idempotent(self):
        operator = OperatorRef.parse("hybrid@1")
        self.assertEqual(OperatorRef.parse(operator), operator)


class RefTests(unittest.TestCase):
    def test_embeddings_with_nothing_new_to_write_were_reused(self):
        reused = EmbeddingsRef(
            document_id=DOCUMENT_ID, embedder="hybrid@1", chunk_count=3, embedded=0
        )
        built = EmbeddingsRef(
            document_id=DOCUMENT_ID, embedder="hybrid@1", chunk_count=3, embedded=3
        )
        self.assertTrue(reused.reused)
        self.assertFalse(built.reused)


class ShapeTests(unittest.TestCase):
    def test_a_chunk_names_its_document_extractor_and_chunker(self):
        chunk = Chunk(
            chunk_id=uuid4(),
            extraction_id=EXTRACTION_ID,
            document_id=DOCUMENT_ID,
            extractor="tika@1",
            chunker="chunker@2",
            index=0,
            text="hello",
        )
        self.assertEqual(chunk.headings, [])
        self.assertEqual(chunk.document_id, DOCUMENT_ID)

    def test_a_draft_has_no_identity_until_the_store_keeps_it(self):
        draft = ChunkDraft(index=0, text="hello")
        self.assertNotIn("chunk_id", draft.model_dump())
        self.assertNotIn("document_id", draft.model_dump())

    def test_document_info_carries_identity_but_no_location(self):
        info = DocumentInfo(
            document_id=DOCUMENT_ID,
            content_id=CONTENT_ID,
            filename="notes.md",
            content_type="text/markdown",
            size=12,
            status=DocumentStatus.UPLOADED,
            published=False,
            created_at=datetime.now(UTC),
        )
        self.assertEqual(info.content_id, CONTENT_ID)
        self.assertNotIn("bucket", info.model_dump())
        self.assertNotIn("object_key", info.model_dump())

if __name__ == "__main__":
    unittest.main()

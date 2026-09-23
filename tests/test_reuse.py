"""The reuse rule and the delete cascade as the database enforces them, which only a real
database can show.

The store owns every table, and the unique key on each is the whole reuse mechanism: no
lease, no attempt counter, no in-flight row. Two runs that do the same work
at the same time converge on one copy rather than one waiting on the other's lock. Every
table hangs off the document by a cascading foreign key, so deleting the document deletes
everything derived from it.

Skipped when no PostgreSQL is reachable, so the rest of the suite stays offline.
"""

import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import delete, func, select
from sqlalchemy.exc import SQLAlchemyError

from contracts.chunks import ChunkDraft
from contracts.embeddings import Embedding, SearchOptions, SparseVector
from contracts.operators import OperatorRef
from db import SessionLocal
from store import models, repository
from store.models import Document
from store.schema import ensure_schema
from store.service import DocumentStore

EXTRACTOR = OperatorRef(name="tika", version="1")
EXTRACTOR_V2 = OperatorRef(name="tika", version="2")
CHUNKER = OperatorRef(name="chunker", version="2")
CHUNKER_V3 = OperatorRef(name="chunker", version="3")
EMBEDDER = OperatorRef(name="hybrid", version="1")
EMBEDDER_V2 = OperatorRef(name="hybrid", version="2")
PIECES = [
    ChunkDraft(index=0, headings=["One"], text="first"),
    ChunkDraft(index=1, headings=[], text="second"),
]
EMBEDDING = Embedding(
    dense=[0.1, 0.2], sparse=SparseVector(indices=[0], values=[0.5], dimension=4)
)


def database_is_reachable() -> bool:
    try:
        ensure_schema()
        with SessionLocal() as session:
            session.connection()
        return True
    except (SQLAlchemyError, OSError):
        return False


def count(model, *conditions) -> int:
    with SessionLocal() as session:
        return session.scalar(select(func.count()).select_from(model).where(*conditions))


class ReuseTestCase(unittest.TestCase):
    """A document of its own per test, so cleanup removes only this test's rows."""

    def setUp(self):
        self.content_id = uuid4().hex + uuid4().hex[:32]
        self.document_id = self.add_document(self.content_id)
        self.documents = [self.document_id]
        self.addCleanup(self.remove_documents)

    def add_document(self, content_id):
        document_id = uuid4()
        repository.add_document(
            document_id,
            content_id=content_id,
            title="notes",
            bucket="smart-files",
            object_key=f"documents/{document_id}/notes.md",
            filename="notes.md",
            content_type="text/markdown",
            size=12,
            now=datetime.now(UTC),
        )
        return document_id

    def another_document(self):
        document_id = self.add_document(uuid4().hex + uuid4().hex[:32])
        self.documents.append(document_id)
        return document_id

    def remove_documents(self):
        with SessionLocal.begin() as session:
            session.execute(
                delete(Document).where(Document.document_id.in_(self.documents))
            )

    def extract(self, document_id=None, extractor=EXTRACTOR, extraction_id=None):
        return repository.add_extraction(
            extraction_id or uuid4(),
            document_id or self.document_id,
            extractor,
            mime_type="text/markdown",
            metadata={"title": "Notes"},
        ).extraction_id

    def chunk(self, document_id=None, extractor=EXTRACTOR, chunker=CHUNKER):
        extraction_id = self.extract(document_id, extractor)
        repository.add_chunks(extraction_id, chunker, PIECES)
        return repository.read_chunks(extraction_id, chunker)

    def count_chunks(self, document_id=None, extractor=EXTRACTOR, chunker=CHUNKER):
        extraction = repository.find_extraction(document_id or self.document_id, extractor)
        return repository.count_chunks(extraction.extraction_id, chunker) if extraction else 0

    def pending(self, embedder=EMBEDDER, document_id=None):
        extraction = repository.find_extraction(document_id or self.document_id, EXTRACTOR)
        return repository.unembedded_chunks(extraction.extraction_id, CHUNKER, embedder)

    def embed(self, chunks, embedder=EMBEDDER, embedding=EMBEDDING):
        return repository.add_embeddings(
            embedder, {chunk.chunk_id: embedding for chunk in chunks}
        )


@unittest.skipUnless(database_is_reachable(), "no PostgreSQL available")
class ExtractionReuseTests(ReuseTestCase):
    def test_nothing_is_found_until_the_extraction_is_written(self):
        self.assertIsNone(repository.find_extraction(self.document_id, EXTRACTOR))

    def test_an_extraction_is_read_back_by_document_and_extractor_version(self):
        extraction_id = self.extract()

        found = repository.find_extraction(self.document_id, EXTRACTOR)

        self.assertEqual(found.extraction_id, extraction_id)
        self.assertEqual(found.extractor, "tika@1")
        self.assertEqual(found.mime_type, "text/markdown")
        self.assertEqual(found.document_metadata, {"title": "Notes"})
        self.assertIsNone(repository.find_extraction(self.document_id, EXTRACTOR_V2))

    def test_two_simultaneous_extractions_converge_on_one_row(self):
        candidates = [uuid4() for _ in range(4)]

        with ThreadPoolExecutor(max_workers=4) as pool:
            kept = list(
                pool.map(lambda candidate: self.extract(extraction_id=candidate), candidates)
            )

        # Every run is told the same winner, which is one of the runs' own IDs.
        self.assertEqual(len(set(kept)), 1)
        self.assertIn(kept[0], candidates)
        self.assertEqual(
            count(
                models.Extraction, models.Extraction.document_id == self.document_id
            ),
            1,
        )


@unittest.skipUnless(database_is_reachable(), "no PostgreSQL available")
class ChunkReuseTests(ReuseTestCase):
    def test_nothing_is_readable_until_the_chunks_are_written(self):
        self.extract()

        self.assertEqual(self.count_chunks(), 0)

    def test_chunks_are_read_back_in_order(self):
        chunks = self.chunk()

        self.assertEqual([chunk.index for chunk in chunks], [0, 1])
        self.assertEqual(chunks[0].document_id, self.document_id)
        self.assertEqual(chunks[0].headings, ["One"])
        self.assertEqual(chunks[0].extractor, "tika@1")
        self.assertEqual(chunks[0].chunker, "chunker@2")
        self.assertEqual(self.count_chunks(), 2)

    def test_writing_the_same_chunks_again_changes_nothing(self):
        first = self.chunk()

        second = self.chunk()

        self.assertEqual(
            [chunk.chunk_id for chunk in second], [chunk.chunk_id for chunk in first]
        )

    def test_a_new_chunker_version_cuts_its_own_chunks(self):
        self.chunk()

        self.chunk(chunker=CHUNKER_V3)

        self.assertEqual(self.count_chunks(), 2)
        self.assertEqual(
            self.count_chunks(chunker=CHUNKER_V3), 2
        )

    def test_a_new_extractor_version_is_new_text_and_so_new_chunks(self):
        self.chunk()

        self.chunk(extractor=EXTRACTOR_V2)

        self.assertEqual(self.count_chunks(), 2)
        self.assertEqual(
            self.count_chunks(extractor=EXTRACTOR_V2), 2
        )

    def test_a_different_document_has_its_own_chunks(self):
        self.chunk()
        other = self.another_document()

        self.assertEqual(self.count_chunks(other), 0)

    def test_two_simultaneous_writes_converge_on_one_set_of_chunks(self):
        # No lease decides this: both runs do the work, and the unique key decides which
        # rows survive. Neither waits on the other, and neither is locked out.
        extraction_id = self.extract()
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = [
                pool.submit(repository.add_chunks, extraction_id, CHUNKER, PIECES)
                for _ in range(2)
            ]
            counts = [result.result() for result in results]

        self.assertEqual(counts, [2, 2])
        self.assertEqual(
            count(models.Chunk, models.Chunk.extraction_id == extraction_id), 2
        )


@unittest.skipUnless(database_is_reachable(), "no PostgreSQL available")
class EmbeddingReuseTests(ReuseTestCase):
    def setUp(self):
        super().setUp()
        self.chunks = self.chunk()
        self.chunk_ids = [chunk.chunk_id for chunk in self.chunks]

    def test_embedded_chunks_are_no_longer_pending(self):
        self.embed(self.chunks[:1])

        pending = self.pending()

        self.assertEqual([chunk.chunk_id for chunk in pending], self.chunk_ids[1:])

    def test_embedding_the_same_chunk_again_changes_nothing(self):
        self.embed(self.chunks)
        self.embed(self.chunks)

        self.assertEqual(count(models.Embedding, models.Embedding.chunk_id.in_(self.chunk_ids)), 2)

    def test_a_new_embedder_version_embeds_on_its_own(self):
        self.embed(self.chunks)

        pending = self.pending(EMBEDDER_V2)
        self.assertEqual([chunk.chunk_id for chunk in pending], self.chunk_ids)

    def test_two_simultaneous_writes_converge_on_one_embedding_per_chunk(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = [pool.submit(self.embed, self.chunks) for _ in range(2)]
            [result.result() for result in results]

        self.assertEqual(count(models.Embedding, models.Embedding.chunk_id.in_(self.chunk_ids)), 2)


@unittest.skipUnless(database_is_reachable(), "no PostgreSQL available")
class DocumentIdentityTests(ReuseTestCase):
    def commit(self, candidate, content_id):
        document = repository.add_document(
            candidate,
            content_id=content_id,
            title="notes",
            bucket="smart-files",
            object_key=f"documents/{candidate}/notes.md",
            filename="notes.md",
            content_type="text/markdown",
            size=12,
            now=datetime.now(UTC),
        )
        self.documents.append(candidate)
        return document

    def test_a_new_candidate_becomes_the_document(self):
        candidate = uuid4()

        document = self.commit(candidate, uuid4().hex + uuid4().hex[:32])

        self.assertEqual(document.document_id, candidate)

    def test_a_candidate_whose_bytes_are_known_resolves_to_that_document(self):
        document = self.commit(uuid4(), self.content_id)

        self.assertEqual(document.document_id, self.document_id)
        self.assertEqual(count(Document, Document.content_id == self.content_id), 1)

    def test_committing_a_candidate_twice_finds_its_own_row(self):
        candidate = uuid4()
        content_id = uuid4().hex + uuid4().hex[:32]
        self.commit(candidate, content_id)

        self.assertEqual(self.commit(candidate, content_id).document_id, candidate)

    def test_racing_candidates_with_the_same_bytes_converge_on_one_document(self):
        content_id = uuid4().hex + uuid4().hex[:32]
        candidates = [uuid4() for _ in range(4)]

        with ThreadPoolExecutor(max_workers=4) as pool:
            documents = list(
                pool.map(lambda candidate: self.commit(candidate, content_id), candidates)
            )

        self.assertEqual(len({document.document_id for document in documents}), 1)
        self.assertIn(documents[0].document_id, candidates)
        self.assertEqual(count(Document, Document.content_id == content_id), 1)

    def test_reports_which_ids_are_documents(self):
        self.assertEqual(
            repository.existing_document_ids([self.document_id, uuid4()]),
            {self.document_id},
        )
        self.assertEqual(repository.existing_document_ids([]), set())


@unittest.skipUnless(database_is_reachable(), "no PostgreSQL available")
class DeleteCascadeTests(ReuseTestCase):
    def test_deleting_a_document_deletes_everything_derived_from_it(self):
        chunks = self.chunk()
        self.embed(chunks)
        chunk_ids = [chunk.chunk_id for chunk in chunks]
        other = self.another_document()
        self.embed(self.chunk(document_id=other))
        removed = []

        deleted = repository.delete_document(
            self.document_id, lambda document: removed.append(document.document_id)
        )

        self.assertTrue(deleted)
        self.assertEqual(removed, [self.document_id])
        self.assertIsNone(repository.get_document(self.document_id))
        self.assertIsNone(repository.find_extraction(self.document_id, EXTRACTOR))
        self.assertEqual(count(models.Chunk, models.Chunk.chunk_id.in_(chunk_ids)), 0)
        self.assertEqual(count(models.Embedding, models.Embedding.chunk_id.in_(chunk_ids)), 0)
        # Nothing belonging to another document is touched.
        self.assertEqual(self.count_chunks(other), 2)

    def test_deleting_an_extraction_deletes_its_chunks_and_embeddings(self):
        chunks = self.chunk()
        self.embed(chunks)
        chunk_ids = [chunk.chunk_id for chunk in chunks]

        with SessionLocal.begin() as session:
            session.execute(
                delete(models.Extraction).where(
                    models.Extraction.extraction_id == chunks[0].extraction_id
                )
            )

        self.assertEqual(count(models.Chunk, models.Chunk.chunk_id.in_(chunk_ids)), 0)
        self.assertEqual(count(models.Embedding, models.Embedding.chunk_id.in_(chunk_ids)), 0)
        self.assertIsNotNone(repository.get_document(self.document_id))

    def test_a_failure_removing_objects_rolls_the_rows_back(self):
        chunks = self.chunk()
        self.embed(chunks)

        def fail(_document):
            raise RuntimeError("s3 is down")

        with self.assertRaises(RuntimeError):
            repository.delete_document(self.document_id, fail)

        self.assertIsNotNone(repository.get_document(self.document_id))
        self.assertEqual(self.count_chunks(), 2)
        self.assertEqual(
            self.pending(), []
        )

    def test_deleting_an_unknown_document_reports_it(self):
        self.assertFalse(
            repository.delete_document(uuid4(), lambda *_args: None)
        )


@unittest.skipUnless(database_is_reachable(), "no PostgreSQL available")
class SearchTests(ReuseTestCase):
    def setUp(self):
        super().setUp()
        # An embedder version of this test's own, so nothing else in the database ranks.
        self.embedder = OperatorRef(name="search-test", version=uuid4().hex)
        self.chunks = self.chunk()
        self.embed(self.chunks[:1], self.embedder, near(0))
        self.embed(self.chunks[1:], self.embedder, near(1))
        repository.mark_published(self.document_id)

    def search(self, query, embedder=None, limit=3):
        options = SearchOptions(
            embedder=embedder or self.embedder, limit=limit, candidates=50, fusion_k=60
        )
        return DocumentStore().search_embeddings(query, options)

    def test_finds_the_nearest_chunk_with_its_document(self):
        hits = self.search(near(1))

        self.assertEqual(hits[0].chunk.chunk_id, self.chunks[1].chunk_id)
        self.assertEqual(hits[0].chunk.text, "second")
        self.assertEqual(hits[0].chunk.document_id, self.document_id)
        self.assertEqual(hits[0].document_name, "notes")
        self.assertGreater(hits[0].score, hits[1].score)

    def test_returns_no_more_than_the_limit(self):
        self.assertEqual(len(self.search(near(0), limit=1)), 1)

    def test_is_scoped_to_one_embedder_version(self):
        other = OperatorRef(name="search-test", version=uuid4().hex)
        self.assertEqual(self.search(near(0), embedder=other), [])

    def test_leaves_out_a_document_that_is_not_published(self):
        other = self.another_document()
        self.embed(self.chunk(document_id=other), self.embedder, near(0))

        hits = self.search(near(0), limit=10)

        self.assertEqual({hit.chunk.document_id for hit in hits}, {self.document_id})


@unittest.skipUnless(database_is_reachable(), "no PostgreSQL available")
class DocumentStatusTests(ReuseTestCase):
    def test_a_new_document_is_uploaded_and_unpublished(self):
        document = repository.get_document(self.document_id)

        self.assertEqual(document.status, "uploaded")
        self.assertIsNone(document.error)
        self.assertFalse(document.published)

    def test_any_status_can_follow_any_other(self):
        repository.set_status(self.document_id, "failed", {"detail": "tika down"})
        repository.set_status(self.document_id, "ingesting")

        document = repository.get_document(self.document_id)
        self.assertEqual(document.status, "ingesting")
        self.assertIsNone(document.error)

    def test_a_failed_run_leaves_a_published_document_published(self):
        repository.mark_published(self.document_id)
        repository.set_status(self.document_id, "failed", {"detail": "no gpu"})

        document = repository.get_document(self.document_id)
        self.assertEqual(document.status, "failed")
        self.assertTrue(document.published)

    def test_a_status_for_a_deleted_document_is_dropped(self):
        self.assertFalse(repository.set_status(uuid4(), "ok"))
        self.assertFalse(repository.mark_published(uuid4()))


def near(position):
    """An embedding pointing at one axis, so the nearest chunk is unambiguous."""
    dense = [0.0, 0.0]
    dense[position] = 1.0
    return Embedding(
        dense=dense,
        sparse=SparseVector(indices=[position], values=[1.0], dimension=4),
    )


if __name__ == "__main__":
    unittest.main()

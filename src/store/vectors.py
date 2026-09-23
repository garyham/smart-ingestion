"""The raw SQL behind the embeddings table.

pgvector's ranking operators have no ORM spelling, so writes and searches are written out
here. Search is always scoped to one embedder version, so a second version can run beside
the first without its results appearing alongside them.
"""

from collections.abc import Mapping
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from sqlalchemy import text

from contracts.chunks import Chunk
from contracts.embeddings import Embedding, SearchHit, SearchOptions, SparseVector
from contracts.operators import OperatorRef
from db import DATABASE_URL, SCHEMA


def connect():
    return psycopg.connect(DATABASE_URL)


def dense_vector_literal(values: list[float]) -> str:
    return "[" + ",".join(str(float(value)) for value in values) + "]"


def sparse_vector_literal(vector: SparseVector) -> str:
    # FastEmbed token IDs are zero-based. pgvector sparsevec positions are one-based.
    entries = ",".join(
        f"{index + 1}:{float(value)}"
        for index, value in sorted(zip(vector.indices, vector.values, strict=True))
    )
    return f"{{{entries}}}/{vector.dimension}"


def write(
    session, embedder: OperatorRef, embeddings: Mapping[UUID, Embedding]
) -> int:
    """Insert embeddings on the caller's transaction, skipping any already there.

    An immutable embedder version embeds a chunk the same way every time, so a row that
    conflicts with an earlier run's is dropped rather than replaced.
    """
    rows = [
        {
            "chunk_id": chunk_id,
            "embedder_name": embedder.name,
            "embedder_version": embedder.version,
            "dense": dense_vector_literal(embedding.dense),
            "sparse": sparse_vector_literal(embedding.sparse),
        }
        for chunk_id, embedding in embeddings.items()
    ]
    if rows:
        session.execute(
            text(
                f"""
                INSERT INTO {SCHEMA}.embeddings (
                    chunk_id, embedder_name, embedder_version,
                    dense_embedding, sparse_embedding
                )
                VALUES (
                    :chunk_id, :embedder_name, :embedder_version,
                    CAST(:dense AS vector), CAST(:sparse AS sparsevec)
                )
                ON CONFLICT DO NOTHING
                """
            ),
            rows,
        )
    return len(rows)


def search(query: Embedding, options: SearchOptions) -> list[SearchHit]:
    """Rank one embedder version's embeddings both ways and fuse the two rankings."""
    dense_literal = dense_vector_literal(query.dense)
    sparse_literal = sparse_vector_literal(query.sparse)

    with connect() as connection, connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            f"""
            WITH candidates AS (
                -- Only published documents: one still being built is not searchable.
                SELECT embeddings.chunk_id, embeddings.dense_embedding,
                       embeddings.sparse_embedding
                FROM {SCHEMA}.embeddings AS embeddings
                JOIN {SCHEMA}.chunks AS chunks USING (chunk_id)
                JOIN {SCHEMA}.extractions AS extractions USING (extraction_id)
                JOIN {SCHEMA}.documents AS documents USING (document_id)
                WHERE embeddings.embedder_name = %s
                  AND embeddings.embedder_version = %s
                  AND documents.published
            ),
            dense AS (
                SELECT chunk_id,
                       row_number() OVER (
                           ORDER BY dense_embedding <=> %s::vector
                       ) AS rank
                FROM candidates
                ORDER BY dense_embedding <=> %s::vector
                LIMIT %s
            ),
            sparse AS (
                SELECT chunk_id,
                       row_number() OVER (
                           ORDER BY sparse_embedding <=> %s::sparsevec
                       ) AS rank
                FROM candidates
                ORDER BY sparse_embedding <=> %s::sparsevec
                LIMIT %s
            ),
            fused AS (
                SELECT COALESCE(dense.chunk_id, sparse.chunk_id) AS chunk_id,
                       COALESCE(1.0 / (%s + dense.rank), 0) +
                       COALESCE(1.0 / (%s + sparse.rank), 0) AS score
                FROM dense
                FULL OUTER JOIN sparse USING (chunk_id)
            )
            SELECT chunks.chunk_id, chunks.extraction_id, extractions.document_id,
                   extractions.extractor_name, extractions.extractor_version,
                   chunks.chunker_name, chunks.chunker_version, chunks.chunk_index,
                   chunks.headings, chunks.text, documents.title AS document_name,
                   fused.score
            FROM fused
            JOIN {SCHEMA}.chunks AS chunks USING (chunk_id)
            JOIN {SCHEMA}.extractions AS extractions USING (extraction_id)
            JOIN {SCHEMA}.documents AS documents USING (document_id)
            ORDER BY fused.score DESC, chunks.chunk_id
            LIMIT %s
            """,
            (
                options.embedder.name,
                options.embedder.version,
                dense_literal,
                dense_literal,
                options.candidates,
                sparse_literal,
                sparse_literal,
                options.candidates,
                options.fusion_k,
                options.fusion_k,
                options.limit,
            ),
        )
        return [_hit(row) for row in cursor.fetchall()]


def _hit(row: dict) -> SearchHit:
    return SearchHit(
        chunk=Chunk(
            chunk_id=row["chunk_id"],
            extraction_id=row["extraction_id"],
            document_id=row["document_id"],
            extractor=f"{row['extractor_name']}@{row['extractor_version']}",
            chunker=f"{row['chunker_name']}@{row['chunker_version']}",
            index=row["chunk_index"],
            headings=row["headings"],
            text=row["text"],
        ),
        document_name=row["document_name"],
        score=float(row["score"]),
    )

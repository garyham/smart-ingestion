import os
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://prefect:prefect@127.0.0.1:5433/prefect"
)
EMBEDDING_SCHEMA = "smart_files"


def connect():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def ensure_embedding_schema(cursor) -> None:
    """Create the pgvector extension and the idempotent embedding target table."""
    cursor.execute("CREATE EXTENSION IF NOT EXISTS vector")
    cursor.execute(
        f"""
        CREATE SCHEMA IF NOT EXISTS {EMBEDDING_SCHEMA};

        CREATE TABLE IF NOT EXISTS {EMBEDDING_SCHEMA}.chunk_embeddings (
            ingestion_id uuid NOT NULL,
            document_id uuid NOT NULL,
            chunk_index integer NOT NULL,
            headings jsonb NOT NULL DEFAULT '[]'::jsonb,
            content text NOT NULL,
            dense_model text NOT NULL,
            sparse_model text NOT NULL,
            dense_embedding vector NOT NULL,
            sparse_embedding sparsevec NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (
                ingestion_id,
                chunk_index,
                dense_model,
                sparse_model
            )
        );

        CREATE INDEX IF NOT EXISTS chunk_embeddings_document_idx
            ON {EMBEDDING_SCHEMA}.chunk_embeddings (document_id, ingestion_id);
        """
    )


def ensure_schema() -> None:
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext('smart_files_schema'))")
        ensure_embedding_schema(cursor)


def dense_vector_literal(values: list[float]) -> str:
    return "[" + ",".join(str(float(value)) for value in values) + "]"


def sparse_vector_literal(vector: dict[str, Any]) -> str:
    indices = vector["indices"]
    values = vector["values"]
    dimension = int(vector["dimension"])
    if len(indices) != len(values):
        raise ValueError("sparse indices and values have different lengths")
    if dimension < 1 or dimension > 1_000_000_000:
        raise ValueError("sparse dimension is outside pgvector's supported range")
    if any(int(index) < 0 or int(index) >= dimension for index in indices):
        raise ValueError("sparse index is outside the declared dimension")

    # FastEmbed token IDs are zero-based. pgvector sparsevec positions are one-based.
    entries = ",".join(
        f"{int(index) + 1}:{float(value)}"
        for index, value in sorted(zip(indices, values, strict=True))
    )
    return f"{{{entries}}}/{dimension}"


def upsert_embeddings(
    document_id: str,
    ingestion_id: str,
    chunks: list[dict],
    dense_vectors: list[list[float]],
    sparse_vectors: list[dict],
    dense_model: str,
    sparse_model: str,
) -> None:
    """Write one document atomically. A raised error rolls back every chunk."""
    if not (len(chunks) == len(dense_vectors) == len(sparse_vectors)):
        raise ValueError("chunk and embedding counts do not match")

    rows = [
        (
            UUID(ingestion_id),
            UUID(document_id),
            int(chunk["index"]),
            Jsonb(chunk.get("headings", [])),
            chunk["text"],
            dense_model,
            sparse_model,
            dense_vector_literal(dense),
            sparse_vector_literal(sparse),
        )
        for chunk, dense, sparse in zip(
            chunks, dense_vectors, sparse_vectors, strict=True
        )
    ]

    with connect() as connection, connection.cursor() as cursor:
        ensure_embedding_schema(cursor)
        cursor.executemany(
            f"""
            INSERT INTO {EMBEDDING_SCHEMA}.chunk_embeddings (
                ingestion_id, document_id, chunk_index, headings, content,
                dense_model, sparse_model, dense_embedding, sparse_embedding
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::vector, %s::sparsevec)
            ON CONFLICT (
                ingestion_id, chunk_index, dense_model, sparse_model
            ) DO UPDATE SET
                document_id = EXCLUDED.document_id,
                headings = EXCLUDED.headings,
                content = EXCLUDED.content,
                dense_embedding = EXCLUDED.dense_embedding,
                sparse_embedding = EXCLUDED.sparse_embedding,
                updated_at = now()
            """,
            rows,
        )


def hybrid_search(
    dense_vector: list[float],
    sparse_vector: dict[str, Any],
    dense_model: str,
    sparse_model: str,
    limit: int = 3,
) -> list[dict]:
    """Combine dense and sparse rankings with reciprocal-rank fusion."""
    candidate_limit = max(limit * 5, 50)
    dense_literal = dense_vector_literal(dense_vector)
    sparse_literal = sparse_vector_literal(sparse_vector)

    with connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            f"""
            WITH dense AS (
                SELECT ingestion_id, chunk_index,
                       row_number() OVER (
                           ORDER BY dense_embedding <=> %s::vector
                       ) AS rank,
                       1 - (dense_embedding <=> %s::vector) AS score
                FROM {EMBEDDING_SCHEMA}.chunk_embeddings
                WHERE dense_model = %s AND sparse_model = %s
                ORDER BY dense_embedding <=> %s::vector
                LIMIT %s
            ),
            sparse AS (
                SELECT ingestion_id, chunk_index,
                       row_number() OVER (
                           ORDER BY sparse_embedding <=> %s::sparsevec
                       ) AS rank,
                       1 - (sparse_embedding <=> %s::sparsevec) AS score
                FROM {EMBEDDING_SCHEMA}.chunk_embeddings
                WHERE dense_model = %s AND sparse_model = %s
                ORDER BY sparse_embedding <=> %s::sparsevec
                LIMIT %s
            ),
            fused AS (
                SELECT COALESCE(dense.ingestion_id, sparse.ingestion_id) AS ingestion_id,
                       COALESCE(dense.chunk_index, sparse.chunk_index) AS chunk_index,
                       dense.score AS dense_score,
                       sparse.score AS sparse_score,
                       COALESCE(1.0 / (60 + dense.rank), 0) +
                       COALESCE(1.0 / (60 + sparse.rank), 0) AS score
                FROM dense
                FULL OUTER JOIN sparse USING (ingestion_id, chunk_index)
            )
            SELECT chunks.document_id, chunks.ingestion_id, chunks.chunk_index,
                   chunks.headings, chunks.content, fused.score,
                   fused.dense_score, fused.sparse_score
            FROM fused
            JOIN {EMBEDDING_SCHEMA}.chunk_embeddings AS chunks
              USING (ingestion_id, chunk_index)
            WHERE chunks.dense_model = %s AND chunks.sparse_model = %s
            ORDER BY fused.score DESC, chunks.ingestion_id, chunks.chunk_index
            LIMIT %s
            """,
            (
                dense_literal,
                dense_literal,
                dense_model,
                sparse_model,
                dense_literal,
                candidate_limit,
                sparse_literal,
                sparse_literal,
                dense_model,
                sparse_model,
                sparse_literal,
                candidate_limit,
                dense_model,
                sparse_model,
                limit,
            ),
        )
        return list(cursor.fetchall())

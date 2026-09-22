import hashlib
import os
from pathlib import Path
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://prefect:prefect@127.0.0.1:5433/prefect"
)
PIPELINE_VERSION = os.getenv("PIPELINE_VERSION", "1")
INGESTION_SCHEMA = "smart_files"


def connect():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def ensure_ingestion_schema(cursor) -> None:
    cursor.execute(
        f"""
        CREATE SCHEMA IF NOT EXISTS {INGESTION_SCHEMA};

        CREATE TABLE IF NOT EXISTS {INGESTION_SCHEMA}.ingestions (
            ingestion_id uuid PRIMARY KEY,
            document_id uuid NOT NULL,
            source_sha256 text NOT NULL CHECK (source_sha256 ~ '^[0-9a-f]{{64}}$'),
            pipeline_version text NOT NULL,
            status text NOT NULL CHECK (
                status IN ('processing', 'published', 'embedding', 'completed', 'failed')
            ),
            current_step text NOT NULL,
            source jsonb NOT NULL,
            steps jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            outputs jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            error jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            completed_at timestamptz,
            UNIQUE (source_sha256, pipeline_version)
        );

        CREATE INDEX IF NOT EXISTS ingestions_status_idx
            ON {INGESTION_SCHEMA}.ingestions (status, updated_at);
        """
    )


def ensure_schema() -> None:
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext('smart_files_schema'))")
        ensure_ingestion_schema(cursor)


def claim_ingestion(
    ingestion_id: str,
    document_id: str,
    source_sha256: str,
    source: dict,
) -> tuple[bool, dict]:
    """Claim content for this pipeline version, or return its canonical ingestion."""
    with connect() as connection, connection.cursor() as cursor:
        ensure_ingestion_schema(cursor)
        cursor.execute(
            f"""
            INSERT INTO {INGESTION_SCHEMA}.ingestions (
                ingestion_id, document_id, source_sha256, pipeline_version,
                status, current_step, source, steps
            )
            VALUES (
                %s, %s, %s, %s, 'processing', 'routing', %s,
                jsonb_build_object(
                    'routing', jsonb_build_object('status', 'processing', 'updated_at', now())
                )
            )
            ON CONFLICT (source_sha256, pipeline_version) DO NOTHING
            RETURNING *
            """,
            (
                UUID(ingestion_id),
                UUID(document_id),
                source_sha256,
                PIPELINE_VERSION,
                Jsonb(source),
            ),
        )
        row = cursor.fetchone()
        if row:
            return True, dict(row)

        cursor.execute(
            f"""
            SELECT * FROM {INGESTION_SCHEMA}.ingestions
            WHERE source_sha256 = %s AND pipeline_version = %s
            FOR UPDATE
            """,
            (source_sha256, PIPELINE_VERSION),
        )
        row = dict(cursor.fetchone())
        owned = str(row["ingestion_id"]) == ingestion_id
        if owned and row["status"] == "failed":
            cursor.execute(
                f"""
                UPDATE {INGESTION_SCHEMA}.ingestions
                SET status = 'processing', current_step = 'routing', error = NULL,
                    steps = steps || jsonb_build_object(
                        'routing', jsonb_build_object(
                            'status', 'processing', 'updated_at', now()
                        )
                    ),
                    updated_at = now(), completed_at = NULL
                WHERE ingestion_id = %s
                RETURNING *
                """,
                (UUID(ingestion_id),),
            )
            row = dict(cursor.fetchone())
        return owned, row


def update_ingestion(
    ingestion_id: str,
    status: str,
    current_step: str,
    *,
    outputs: dict | None = None,
    error: dict | None = None,
) -> None:
    completed = status == "completed"
    with connect() as connection, connection.cursor() as cursor:
        ensure_ingestion_schema(cursor)
        cursor.execute(
            f"""
            UPDATE {INGESTION_SCHEMA}.ingestions
            SET status = %s,
                current_step = %s,
                outputs = outputs || %s,
                steps = steps || jsonb_build_object(
                    %s, jsonb_build_object('status', %s, 'updated_at', now())
                ),
                error = %s,
                updated_at = now(),
                completed_at = CASE WHEN %s THEN now() ELSE completed_at END
            WHERE ingestion_id = %s
            """,
            (
                status,
                current_step,
                Jsonb(outputs or {}),
                current_step,
                status,
                Jsonb(error) if error is not None else None,
                completed,
                UUID(ingestion_id),
            ),
        )

import os
from datetime import timedelta
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://prefect:prefect@127.0.0.1:5433/prefect"
)
QUEUE_SCHEMA = "smart_files"


def connect():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def ensure_schema() -> None:
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext('smart_files_schema'))")
        cursor.execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS {QUEUE_SCHEMA};

            CREATE TABLE IF NOT EXISTS {QUEUE_SCHEMA}.ingestion_jobs (
                id uuid PRIMARY KEY,
                event jsonb NOT NULL,
                status text NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
                attempts integer NOT NULL DEFAULT 0,
                available_at timestamptz NOT NULL DEFAULT now(),
                locked_until timestamptz,
                worker_id text,
                last_error text,
                created_at timestamptz NOT NULL DEFAULT now(),
                updated_at timestamptz NOT NULL DEFAULT now()
            );

            CREATE INDEX IF NOT EXISTS ingestion_jobs_ready_idx
                ON {QUEUE_SCHEMA}.ingestion_jobs (available_at, created_at)
                WHERE status = 'pending';

            CREATE INDEX IF NOT EXISTS ingestion_jobs_expired_idx
                ON {QUEUE_SCHEMA}.ingestion_jobs (locked_until)
                WHERE status = 'processing';

            CREATE TABLE IF NOT EXISTS {QUEUE_SCHEMA}.outbox (
                id uuid PRIMARY KEY,
                topic text NOT NULL,
                event jsonb NOT NULL,
                status text NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'processing', 'processed', 'failed')),
                attempts integer NOT NULL DEFAULT 0,
                available_at timestamptz NOT NULL DEFAULT now(),
                locked_until timestamptz,
                consumer_id text,
                last_error text,
                created_at timestamptz NOT NULL DEFAULT now(),
                processed_at timestamptz
            );

            CREATE INDEX IF NOT EXISTS outbox_ready_idx
                ON {QUEUE_SCHEMA}.outbox (topic, available_at, created_at)
                WHERE status = 'pending';
            """
        )


def enqueue_upload(event: dict[str, Any]) -> None:
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            f"""
            INSERT INTO {QUEUE_SCHEMA}.ingestion_jobs (id, event)
            VALUES (%s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (UUID(str(event["event_id"])), Jsonb(event)),
        )


def claim_upload(worker_id: str, lease_seconds: int) -> dict[str, Any] | None:
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            f"""
            WITH next_job AS (
                SELECT id
                FROM {QUEUE_SCHEMA}.ingestion_jobs
                WHERE available_at <= now()
                  AND (
                    status = 'pending'
                    OR (status = 'processing' AND locked_until < now())
                  )
                ORDER BY created_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE {QUEUE_SCHEMA}.ingestion_jobs AS job
            SET status = 'processing',
                attempts = attempts + 1,
                locked_until = now() + (%s * interval '1 second'),
                worker_id = %s,
                updated_at = now()
            FROM next_job
            WHERE job.id = next_job.id
            RETURNING job.id, job.event, job.attempts
            """,
            (lease_seconds, worker_id),
        )
        return cursor.fetchone()


def extend_upload_lease(job_id: UUID, worker_id: str, lease_seconds: int) -> bool:
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            f"""
            UPDATE {QUEUE_SCHEMA}.ingestion_jobs
            SET locked_until = now() + (%s * interval '1 second'), updated_at = now()
            WHERE id = %s AND status = 'processing' AND worker_id = %s
            """,
            (lease_seconds, job_id, worker_id),
        )
        return cursor.rowcount == 1


def complete_upload(
    job_id: UUID, worker_id: str, completion_event: dict[str, Any]
) -> bool:
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            f"""
            UPDATE {QUEUE_SCHEMA}.ingestion_jobs
            SET status = 'completed', locked_until = NULL, worker_id = NULL,
                updated_at = now()
            WHERE id = %s AND status = 'processing' AND worker_id = %s
            RETURNING id
            """,
            (job_id, worker_id),
        )
        if cursor.fetchone() is None:
            return False
        cursor.execute(
            f"""
            INSERT INTO {QUEUE_SCHEMA}.outbox (id, topic, event)
            VALUES (%s, 'ingestion.completed', %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (UUID(str(completion_event["ingestion_id"])), Jsonb(completion_event)),
        )
        return True


def fail_upload(
    job_id: UUID,
    worker_id: str,
    error: str,
    attempts: int,
    max_attempts: int,
) -> None:
    failed = attempts >= max_attempts
    delay = min(300, 2 ** min(attempts, 8))
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            f"""
            UPDATE {QUEUE_SCHEMA}.ingestion_jobs
            SET status = %s,
                available_at = now() + %s,
                locked_until = NULL,
                worker_id = NULL,
                last_error = %s,
                updated_at = now()
            WHERE id = %s AND status = 'processing' AND worker_id = %s
            """,
            (
                "failed" if failed else "pending",
                timedelta(seconds=delay),
                error[:4000],
                job_id,
                worker_id,
            ),
        )


def completed_ingestion_ids(expected: set[str]) -> set[str]:
    if not expected:
        return set()
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT event->>'ingestion_id' AS ingestion_id
            FROM {QUEUE_SCHEMA}.outbox
            WHERE topic = 'ingestion.completed'
              AND event->>'ingestion_id' = ANY(%s)
            """,
            (list(expected),),
        )
        return {row["ingestion_id"] for row in cursor.fetchall()}

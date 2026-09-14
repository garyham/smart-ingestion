# PostgreSQL queues

The application uses the `smart_files` schema in the same PostgreSQL database as Prefect. Prefect
does not own this schema.

## Upload queue

`smart_files.ingestion_jobs` stores one job for each `file.uploaded` event. A worker claims one
available row with one short transaction:

```sql
WITH next_job AS (
    SELECT id
    FROM smart_files.ingestion_jobs
    WHERE available_at <= now()
      AND (
        status = 'pending'
        OR (status = 'processing' AND locked_until < now())
      )
    ORDER BY created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE smart_files.ingestion_jobs AS job
SET status = 'processing',
    attempts = attempts + 1,
    locked_until = now() + interval '5 minutes',
    worker_id = :worker_id
FROM next_job
WHERE job.id = next_job.id
RETURNING job.*;
```

`FOR UPDATE` prevents two workers from claiming the same row. `SKIP LOCKED` lets other workers skip
that row instead of waiting. The update and lock are committed before ingestion starts. A database
transaction is never held open while a document is processed.

The worker extends `locked_until` while it works. If it stops, the lease expires and another worker
can claim the row. Failures return the row to `pending` with an exponential delay. The row becomes
`failed` after `QUEUE_MAX_ATTEMPTS` attempts. Delivery is at least once, so processing must remain
idempotent.

## Completion outbox

`smart_files.outbox` replaces the `ingestion.completed` RabbitMQ event. After the immutable manifest
is uploaded, one transaction:

1. Marks the upload job as `completed`.
2. Inserts the `ingestion.completed` event into the outbox.

If either statement fails, both roll back. An outbox consumer should claim rows with the same
`FOR UPDATE SKIP LOCKED` and lease pattern, then mark them `processed`. It must deduplicate by the
event `id` or `ingestion_id`.

The table represents one logical downstream subscription. If several independent consumers must
each receive every event, add a delivery row for each subscription or use one PGMQ queue per
subscription.

## Operations

- Keep the partial indexes on ready and expired rows.
- Poll with a short delay when no row is ready. `LISTEN/NOTIFY` can reduce idle polling, but polling
  must remain as the reliable fallback.
- Monitor pending count, oldest pending age, expired leases, attempts, and failed rows.
- Remove or archive old completed jobs and processed outbox rows in small batches.
- Use managed database migrations instead of application startup DDL before production use.

## PGMQ

PGMQ packages the same main ideas as tested PostgreSQL functions. It provides send, read, visibility
timeouts, retry counts, delete, archive, polling, and optional `LISTEN/NOTIFY` support.

PGMQ would reduce our queue SQL and is a good option if the PostgreSQL service can install and
upgrade extensions. The custom tables remain useful when job status, errors, and the atomic
completion outbox are part of the application data model. For this proof of concept, keep the small
custom implementation. Reassess PGMQ if queue features or maintenance code grow.

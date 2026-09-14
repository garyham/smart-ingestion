# smart-files

A proof-of-concept document ingestion pipeline. Files are uploaded to SeaweedFS and RabbitMQ
notifies the ingestion worker. Prefect stores workflow state in PostgreSQL. The worker stores
artifact bundles in SeaweedFS and publishes completion events through RabbitMQ.

## Installation

Requires Python >=3.12 and [`uv`](https://docs.astral.sh/uv/).

```bash
# clone the repo, then from its root:
uv sync
```

This installs the pinned dependencies from `uv.lock` (Prefect, pymupdf4llm, markitdown, duckdb,
langchain_text_splitters, etc.) into a local `.venv`.

## Operation

Start all services from the project root:

```bash
docker compose up -d --build --wait
```

Open `http://127.0.0.1:8000` and select a file.

Use these commands to view service status and logs:

```bash
docker compose ps
docker compose logs -f api ingest prefect
```

The upload process is:

1. The API creates a signed SeaweedFS upload URL.
2. The browser uploads the file directly to SeaweedFS.
3. The API publishes a `file.uploaded` event to the `file-uploads` RabbitMQ exchange.
4. RabbitMQ puts the event in the durable `file-ingestion` queue.
5. The worker downloads the file from SeaweedFS and runs the ingestion flow.
6. The worker uploads an immutable artifact bundle to SeaweedFS.
7. It uploads `manifest.json` last and publishes an `ingestion.completed` event.

The worker acknowledges a message after successful ingestion. It requeues an ingestion error and
rejects an invalid event.

### Service endpoints

| Service | Address | Notes |
|---|---|---|
| Upload page and API | `http://127.0.0.1:8000` | Upload files here |
| Prefect | `http://127.0.0.1:4200` | Flow runs and logs |
| PostgreSQL | `127.0.0.1:5433` | Prefect database; `prefect` / `prefect` |
| RabbitMQ management | `http://127.0.0.1:15673` | `smart_files` / `smart_files` |
| SeaweedFS S3 API | `http://127.0.0.1:8333` | Used by the API and worker |

### Downstream interface

Each upload creates this object-store bundle:

```text
ingested/<document-id>/<ingestion-id>/
  assets/       # copy of the source file
  metadata.json # extracted metadata
  status.json   # ok, needs_intervention, or failed
  chunks.jsonl  # chunked text, when applicable
  *.duckdb      # spreadsheet database, when applicable
  manifest.json # artifact list, hashes, source, and outcome; uploaded last
```

The durable `file-processing` queue receives a persistent event after the manifest is stored:

```json
{
  "event": "ingestion.completed",
  "schema_version": 1,
  "document_id": "uuid",
  "ingestion_id": "uuid",
  "status": "ok",
  "artifact_type": "chunks",
  "manifest_uri": "s3://smart-files/ingested/<document-id>/<ingestion-id>/manifest.json",
  "completed_at": "2026-09-13T12:00:00+00:00"
}
```

Consumers must acknowledge messages only after processing. They must deduplicate by
`ingestion_id`, because RabbitMQ delivery is at least once. Events are also emitted for
`failed` and `needs_intervention` outcomes.

### Configuration

Docker Compose uses these environment variables:

| Variable | Default |
|---|---|
| `S3_ACCESS_KEY_ID` | `smart_files` |
| `S3_SECRET_ACCESS_KEY` | `smart_files_secret` |
| `S3_BUCKET` | `smart-files` |
| `POSTGRES_DB` | `prefect` |
| `POSTGRES_USER` | `prefect` |
| `POSTGRES_PASSWORD` | `prefect` |
| `RABBITMQ_DEFAULT_USER` | `smart_files` |
| `RABBITMQ_DEFAULT_PASS` | `smart_files` |
| `UPLOAD_EXCHANGE` | `file-uploads` |
| `INGEST_QUEUE` | `file-ingestion` |
| `COMPLETION_EXCHANGE` | `ingestion-results` |
| `PROCESS_QUEUE` | `file-processing` |
| `ARTIFACT_PREFIX` | `ingested` |

The PostgreSQL data is stored in the `postgres_data` Docker volume. Existing data in
`~/.prefect/prefect.db` is not migrated or used by the Compose services.

### Supported document types

Anything outside this whitelist (`config/config.yaml`) is routed straight to a `failed`
`status.json` rather than being ingested. Every type gets metadata extraction and a raw copy under
`assets/`, regardless of handling.

| Type              | MIME type(s)                                                                                                 | Converter     | Handling               |
|-------------------|---------------------------------------------------------------------------------------------------------------|---------------|------------------------|
| PDF               | `application/pdf`                                                                                              | pymupdf4llm   | chunked                |
| Word              | `application/msword`, `application/vnd.openxmlformats-officedocument.wordprocessingml.document`                | markitdown    | chunked                |
| OpenDocument Text | `application/vnd.oasis.opendocument.text`                                                                      | markitdown    | chunked                |
| Excel             | `application/vnd.ms-excel`, `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`                 | -             | DuckDB (not chunked)   |
| OpenDocument Sheet| `application/vnd.oasis.opendocument.spreadsheet`                                                                | -             | DuckDB (not chunked)   |
| CSV               | `text/csv`                                                                                                      | markitdown    | chunked                |
| HTML              | `text/html`                                                                                                     | markitdown    | chunked                |
| Markdown          | `text/markdown`                                                                                                 | markitdown    | chunked                |
| Plain text        | `text/plain`                                                                                                    | markitdown    | chunked                |
| Email             | `message/rfc822`                                                                                                | markitdown    | chunked                |

Spreadsheet types (Excel/ODS) are stored as a `.duckdb` artifact and queried with SQL downstream
instead of being split into chunks.

Other useful commands:

```bash
# upload files from data/, publish 100 notifications, wait, then remove the source objects
uv run load-test --count 100

# stop all services
docker compose down

# remove all Prefect flow-run history while the services are running
uv run clean-runs
```

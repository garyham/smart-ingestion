# smart-files

A proof-of-concept document ingestion pipeline. Files are uploaded to SeaweedFS and queued for
ingestion in PostgreSQL. Prefect stores workflow state in PostgreSQL. The ingestion worker stores
artifact bundles in SeaweedFS. An embedding worker generates dense and sparse vectors and stores
them in PostgreSQL with pgvector.

## Installation

Requires Python >=3.12 and [`uv`](https://docs.astral.sh/uv/).

```bash
# clone the repo, then from its root:
uv sync
```

This installs the pinned dependencies from `uv.lock` (Prefect, DuckDB,
langchain_text_splitters, etc.) into a local `.venv`.

## Operation

Start all services from the project root:

```bash
docker compose up --build --watch
```

Compose syncs changes under `src/` into the API and workers. Uvicorn reloads the API, and Compose
restarts the workers. Changes to `pyproject.toml` or `uv.lock` rebuild the affected images.

Open `http://127.0.0.1:8000` and select a file.

Use these commands to view service status and logs:

```bash
docker compose ps
docker compose logs -f api ingest prefect
```

The upload process is:

1. The API creates a signed SeaweedFS upload URL.
2. The browser uploads the file directly to SeaweedFS.
3. The API inserts a `file.uploaded` job into PostgreSQL.
4. A worker claims the job with `FOR UPDATE SKIP LOCKED` and a renewable lease.
5. The worker downloads the file from SeaweedFS and runs the ingestion flow.
6. The worker uploads an immutable artifact bundle to SeaweedFS.
7. It uploads `manifest.json` last and writes an `ingestion.completed` outbox event.
8. The embedding worker generates dense and sparse vectors in parallel.
9. It writes both vector types in one PostgreSQL transaction.

The worker completes a job after successful ingestion. It retries an ingestion error with a delay
and marks invalid or exhausted jobs as failed. See [PostgreSQL queues](docs/postgres-queues.md).

### Service endpoints

| Service | Address | Notes |
|---|---|---|
| Upload page and API | `http://127.0.0.1:8000` | Upload files here |
| Hybrid chunk search | `http://127.0.0.1:8000/query` | Search dense and sparse embeddings |
| Prefect | `http://127.0.0.1:4200` | Flow runs and logs |
| PostgreSQL | `127.0.0.1:5433` | Prefect, queue, and pgvector data; `prefect` / `prefect` |
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

The durable `smart_files.outbox` table receives an event after the manifest is stored:

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

Consumers must mark rows as processed only after successful work. They must deduplicate by
`ingestion_id`, because delivery is at least once. Events are also written for `failed` and
`needs_intervention` outcomes.

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
| `DATABASE_URL` | `postgresql://prefect:prefect@127.0.0.1:5433/prefect` |
| `QUEUE_LEASE_SECONDS` | `300` |
| `QUEUE_MAX_ATTEMPTS` | `5` |
| `QUEUE_POLL_SECONDS` | `1` |
| `ARTIFACT_PREFIX` | `ingested` |
| `TIKA_URL` | `http://tika:9998` |
| `TIKA_TIMEOUT_SECONDS` | `120` |
| `DENSE_EMBEDDING_MODEL` | `BAAI/bge-small-en-v1.5` |
| `SPARSE_EMBEDDING_MODEL` | `Qdrant/bm42-all-minilm-l6-v2-attentions` |
| `EMBEDDING_DEVICE` | `auto` |
| `EMBEDDING_LEASE_SECONDS` | `900` |
| `EMBEDDING_MAX_ATTEMPTS` | `5` |
| `EMBEDDING_POLL_SECONDS` | `1` |

The standard image installs CPU FastEmbed. A GPU deployment must replace it with
`fastembed-gpu` and include compatible NVIDIA CUDA and cuDNN libraries. With
`EMBEDDING_DEVICE=auto`, the worker uses CUDA when ONNX Runtime exposes it and otherwise uses CPU.

The PostgreSQL data is stored in the `postgres_data` Docker volume. Existing data in
`~/.prefect/prefect.db` is not migrated or used by the Compose services.

### Document handling

Apache Tika detects the MIME type and extracts metadata and plain text in one parse. The text is
chunked for downstream use. Every type gets metadata extraction and a raw copy under `assets/`.

| Type              | MIME type(s)                                                                                                 | Converter     | Handling               |
|-------------------|---------------------------------------------------------------------------------------------------------------|---------------|------------------------|
| PDF               | `application/pdf`                                                                                              | Apache Tika   | chunked                |
| Word              | `application/msword`, `application/vnd.openxmlformats-officedocument.wordprocessingml.document`                | Apache Tika   | chunked                |
| OpenDocument Text | `application/vnd.oasis.opendocument.text`                                                                      | Apache Tika   | chunked                |
| Excel             | `application/vnd.ms-excel`, `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`                 | -             | DuckDB (not chunked)   |
| OpenDocument Sheet| `application/vnd.oasis.opendocument.spreadsheet`                                                                | -             | DuckDB (not chunked)   |
| Other Tika formats| varies                                                                                                          | Apache Tika   | chunked                |

Spreadsheet types (Excel/ODS) are stored as a `.duckdb` artifact and queried with SQL downstream
instead of being split into chunks.

Other useful commands:

```bash
# upload files from data/, queue 100 jobs, wait, then remove the source objects
uv run load-test --count 100

# stop all services
docker compose down

# remove all Prefect flow-run history while the services are running
uv run clean-runs
```

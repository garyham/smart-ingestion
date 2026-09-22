# smart-files

A proof-of-concept document ingestion pipeline. Files are uploaded to SeaweedFS and queued as
Prefect background tasks. Long-lived workers store artifact bundles in SeaweedFS, generate dense
and sparse vectors, and store them in PostgreSQL with pgvector.

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
3. The API submits a Prefect `process-upload` background task.
4. A worker downloads the file, calculates its SHA-256, and claims an ingestion row.
5. A matching active or completed ingestion stops duplicate work.
6. The worker uploads an immutable artifact bundle and `manifest.json` last.
7. Successful chunk bundles submit an `embed-document` background task.
8. The embedding worker generates and stores dense and sparse vectors.

Prefect stores task state, applies ingestion retries, and exposes failures in its UI. The API and
workers share `prefect_results`, which stores deferred task parameters and results.

### Service endpoints

| Service | Address | Notes |
|---|---|---|
| Upload page and API | `http://127.0.0.1:8000` | Upload files here |
| Hybrid chunk search | `http://127.0.0.1:8000/query` | Search dense and sparse embeddings |
| Prefect | `http://127.0.0.1:4200` | Flow runs and logs |
| PostgreSQL | `127.0.0.1:5433` | Prefect and pgvector data; `prefect` / `prefect` |
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

The ingestion task returns this completion value after the manifest is stored:

```json
{
  "event": "ingestion.completed",
  "schema_version": 1,
  "document_id": "uuid",
  "ingestion_id": "uuid",
  "pipeline_version": "1",
  "source_sha256": "hex-encoded SHA-256",
  "status": "ok",
  "artifact_type": "chunks",
  "manifest_uri": "s3://smart-files/ingested/<document-id>/<ingestion-id>/manifest.json",
  "completed_at": "2026-09-13T12:00:00+00:00"
}
```

The worker submits embedding only when the status is `ok` and the artifact type is `chunks`.
Immutable bundle paths and embedding upserts make retries safe.

The `smart_files.ingestions` table tracks the pipeline state from source routing through embedding.
Its `steps`, `outputs`, and `error` JSON fields allow the pipeline to gain new steps without a table
migration.
Rows are unique by source SHA-256 and `PIPELINE_VERSION`. Published files are retained when a later
step fails, so the failed step can be retried without rolling back valid work.

Query ingestion state with `GET /ingestions`. It supports `status`, `source_sha256`, `document_id`,
`pipeline_version`, `limit`, and `offset`. Use `GET /ingestions/{ingestion_id}` for one row.

SQLAlchemy defines the ingestion model, Pydantic defines API records, and Alembic owns the table
schema.

### Database migrations

The `smartfiles-init` service applies pending migrations and configures Prefect concurrency limits
once. The API and workers start only after it succeeds. For local migration commands, start the
Smart Files database first:

```bash
docker compose up -d smart-files-postgres
```

Show the current and available revisions:

```bash
uv run alembic current
uv run alembic history
```

Apply all pending migrations:

```bash
uv run alembic upgrade head
```

After changing `src/ingestion/models.py`, create and review a migration before applying it:

```bash
uv run alembic revision --autogenerate -m "describe the schema change"
uv run alembic upgrade head
```

Generated migrations are stored under `src/ingestion/migrations/versions/`. Alembic manages both
ingestion and embedding storage. Embedding migrations use explicit SQL because they contain
pgvector types. Always inspect generated operations before running them.

Application migration state is stored in `smart_files.alembic_version` in the Smart Files
database. Prefect uses a separate PostgreSQL database and manages its own migrations.

Revert the latest migration during development:

```bash
uv run alembic downgrade -1
```

Generate SQL without changing the database:

```bash
uv run alembic upgrade head --sql
```

### Configuration

Docker Compose uses these environment variables:

| Variable | Default |
|---|---|
| `S3_ACCESS_KEY_ID` | `smart_files` |
| `S3_SECRET_ACCESS_KEY` | `smart_files_secret` |
| `S3_BUCKET` | `smart-files` |
| `WEB_ORIGINS` | `http://localhost:8000,http://127.0.0.1:8000,http://0.0.0.0:8000` |
| `PREFECT_POSTGRES_DB` | `prefect` |
| `PREFECT_POSTGRES_USER` | `prefect` |
| `PREFECT_POSTGRES_PASSWORD` | `prefect` |
| `SMART_FILES_POSTGRES_DB` | `smart_files` |
| `SMART_FILES_POSTGRES_USER` | `smart_files` |
| `SMART_FILES_POSTGRES_PASSWORD` | `smart_files` |
| `DATABASE_URL` | `postgresql://smart_files:smart_files@127.0.0.1:5435/smart_files` |
| `PIPELINE_VERSION` | `1` |
| `ARTIFACT_PREFIX` | `ingested` |
| `TIKA_URL` | `http://tika:9998` |
| `TIKA_TIMEOUT_SECONDS` | `120` |
| `DENSE_EMBEDDING_MODEL` | `BAAI/bge-small-en-v1.5` |
| `SPARSE_EMBEDDING_MODEL` | `Qdrant/bm42-all-minilm-l6-v2-attentions` |
| `EMBEDDING_DEVICE` | `auto` |
| `SMART_FILES_API_URL` | `http://127.0.0.1:8000` |

The standard image installs CPU FastEmbed. A GPU deployment must replace it with
`fastembed-gpu` and include compatible NVIDIA CUDA and cuDNN libraries. With
`EMBEDDING_DEVICE=auto`, the worker uses CUDA when ONNX Runtime exposes it and otherwise uses CPU.

Prefect and Smart Files use the `prefect_postgres_data` and `smart_files_postgres_data` Docker
volumes. Existing data in the former `postgres_data` volume is not migrated automatically.

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

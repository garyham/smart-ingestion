# smart-files

A proof-of-concept document ingestion pipeline. Files are uploaded to SeaweedFS and queued as
Prefect background tasks. Long-lived workers store artifact bundles in SeaweedFS, generate dense
and sparse vectors, and store them in PostgreSQL with pgvector.

Ingestion is built from one store (`src/store/`) and stateless, independently versioned operators
(`src/operators/`) - an extractor, a chunker, and an embedder. The store owns every table and object; the
tables are linked by cascading foreign keys to the document they derive from, and a pipeline
release only selects which operator versions to run. See [Store, operators, and
releases](#store-operators-and-releases), and
[Design](#design) for the reasoning behind the less obvious choices.

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

1. The browser hashes the file itself and asks `GET /documents/lookup?sha256=...` whether those
   bytes are already a document. If they are, it says so and uploads nothing. The check is an
   optimisation - it is skipped for very large files and when it fails, and the server hashes
   whatever does arrive regardless.
2. `DocumentStore.create_upload()` hands out a candidate `document_id` and a URL signed for one
   write, of exactly the declared size, to that candidate's key. Nothing is recorded.
3. The browser uploads the file directly to SeaweedFS, sending the headers `/presign` returned.
   A second write to the same key is refused, so the bytes cannot change once they are hashed.
4. `POST /notify` checks, without reading the bytes, that something arrived recently enough to
   commit, then submits a Prefect `process-upload` task carrying the candidate `document_id`.
5. A worker runs the `ingest-document` flow. Its first task, `commit-upload`, hashes the bytes
   and `DocumentStore.add_document()` records the candidate as the document with that hash. The
   hash *is* the document: identical bytes resolve to the document that already exists, and the
   candidate's copy is deleted.
6. The flow marks the document `ingesting` and extracts it: one Tika parse for the MIME type,
   text, and metadata, kept by the store and reused if that extractor version already ran. It
   then dispatches to the path the MIME type belongs to.
7. The `chunk-document` subflow runs find, split, store as separate tasks, reusing chunks
   already cut from the same text by the same chunker version. Spreadsheets take their own path
   and become a DuckDB dataset instead.
8. The worker uploads an immutable artifact bundle and `manifest.json` last.
9. Chunked documents are marked `embedding` and submit an `embed-release` task. Anything with
   nothing to embed is published as soon as its bundle is.
10. The `embed-document` subflow finds the chunks each embedder the active release names has not
    yet embedded, encodes them, and stores them. Once every embedder has run, the document is
    published.

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

After changing `src/store/models.py`, create and review a migration before applying it:

```bash
uv run alembic revision --autogenerate -m "describe the schema change"
uv run alembic upgrade head
```

Generated migrations are stored under `src/store/migrations/versions/`. One chain covers every
table: `src/db.py` holds the shared declarative base, and `env.py` imports the store's models so
Alembic sees one metadata. Embedding migrations use explicit SQL because they contain pgvector
types. Always inspect generated operations before running them.

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
| `S3_ENDPOINT_URL` | `http://127.0.0.1:8333` (the endpoint this process uses) |
| `S3_PUBLIC_ENDPOINT_URL` | `S3_ENDPOINT_URL` (the endpoint presigned URLs are signed for) |
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
| `CATALOG_STALE_CLAIM_SECONDS` | `3600` |
| `SMART_FILES_API_URL` | `http://127.0.0.1:8000` |

The standard image installs CPU FastEmbed. A GPU deployment must replace it with
`fastembed-gpu` and include compatible NVIDIA CUDA and cuDNN libraries. With
`EMBEDDING_DEVICE=auto`, the worker uses CUDA when ONNX Runtime exposes it and otherwise uses CPU.

Prefect and Smart Files use the `prefect_postgres_data` and `smart_files_postgres_data` Docker
volumes. Existing data in the former `postgres_data` volume is not migrated automatically.

### Document handling

Every document gets one full Apache Tika parse, before routing: its MIME type decides the path,
and its text and metadata serve whichever path that is. Every type gets metadata extraction and a
raw copy under `assets/`, as well as the immutable copy the document store keeps.

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
# run the tests (the reuse tests are skipped when no PostgreSQL is reachable)
uv run pytest
uv run ruff check src tests

# upload files from data/ through the real upload path and wait for them
uv run load-test --count 100

# stop all services
docker compose down

# remove all Prefect flow-run history while the services are running
uv run clean-runs
```

## Design

How the system is put together, and the reasoning behind the choices that are not obvious from the
code - including what would have to change for them to be reconsidered.

### Store, operators, and releases

The store owns every table, object key, and storage format. Operators are plain classes that
compute and never touch storage: a Prefect task runs one, then hands its output to the store. The
orchestrator passes references - never a bucket, an object key, a table, or a storage format.

| Part | Versions | Does |
|---|---|---|
| `DocumentStore` (`src/store/`) | - | Owns `documents`, `extractions`, `chunks`, `embeddings`, source bytes, extracted text, and the migrations; searches embeddings |
| Extractors (`src/operators/extractors/`) | `tika@1` | One Tika parse: bytes to MIME type, text, and metadata |
| Chunkers (`src/operators/chunkers/`) | `chunker@2` | Text to `ChunkDraft`s, and nothing else |
| Embedders (`src/operators/embedders/`) | `hybrid@1` | Dense and sparse embeddings, query encoding, and the fusion settings search uses |

The shapes that cross between them live in `src/contracts/`. The data rows themselves carry the
operator that produced them - an extraction records its `extractor_name`/`extractor_version`, a
chunk its extraction and `chunker_name`/`chunker_version`, an embedding its
`embedder_name`/`embedder_version` - and the reuse rule is that table's unique key. There are no
set or registration tables.

The tables form one tree rooted at the document, every edge a cascading foreign key:

```text
documents ──< extractions ──< chunks ──< embeddings
```

`DocumentStore.delete_document()` (`DELETE /documents/{document_id}`) deletes the document
row in a transaction, which cascades to everything above, then deletes the document's objects -
`documents/<id>/`, `extractions/<id>/`, and `ingested/<id>/` - before committing. If removing
the objects fails, the rows roll back and the delete can simply be repeated.

Execution control belongs to Prefect, not to the store. A flow sequences the tasks, and a task is
the unit of retry; the store exposes each stage as a plain method and holds no lease, no attempt
counter, and no in-flight row. A document's chunks, and each batch of embeddings, are written in one
transaction, so a run that dies part-way leaves nothing and the next attempt simply does the work
again. A failure is recorded by Prefect and the document's `status`, never by a claim.

**A document is its content.** The same SHA-256 is the same document, so uploading a file twice
returns the same `document_id` and every artifact already derived from it. There are no document
versions - different bytes are simply a different document.

An operator result is reused when its **operator version** and **input identity** match.
Operator versions are immutable - their configuration is part of what the version means, so a
different model or chunk size is a new version. `PIPELINE_VERSION` is deliberately not part of the
key. Input identity is just the document or chunk ID, because deduplication already happened at the
document layer.

A release is an immutable composition of operator versions, set in `config/config.yaml`:

```yaml
release:
  name: release-1
  extractor: tika@1
  chunker: chunker@2
  embedders:
    - hybrid@1
```

New documents are ingested with the active release, and `POST /query` searches that release's
embedders unless it is given an explicit `embedder` (such as `hybrid@1`). It never searches every
embedding there is. The API embeds the query with the embedder, then
`DocumentStore.search_embeddings()` ranks that version's embeddings and returns each hit as
`{chunk, document_name, score}`; the store never loads a model. Adding an embedder to a release
does not re-run chunking, and changing the active release does not start a backfill.

Two identities are kept apart: `content_id` (the SHA-256 that decides what a document *is*) and
`document_id` (the surrogate key that names it in a reference). `POST /presign` returns a
*candidate* `document_id`, and `POST /notify` returns the Prefect task run that will hash the
bytes. The candidate becomes a document once that run's `commit-upload` task has recorded it; if
the bytes were already a document, the candidate is discarded and never gets a row, so a client
holding it gets `404` and should look the bytes up instead. `GET /documents/lookup` answers the one question a
client can ask before uploading - whether a `content_id` is already a document - and nothing more:
a hash is not proof of possession, so it grants no access to the bytes behind it.

### Object layout

```text
documents/<document-id>/<filename>       # written once by the client; kept if the hash is new
```

Chunks and embeddings live in PostgreSQL, not in the object store. Chunks are read through
`DocumentStore.chunks()` and `DocumentStore.get_chunks()`.

A candidate's bytes that turn out to be a duplicate are deleted when it is committed. Bytes whose
candidate never became a document - the client never called `/notify`, or deleting a duplicate
failed - are removed by the `discard-abandoned-uploads` flow, which the `sweep` service serves
hourly. `commit-upload` refuses bytes older than 12 hours and the sweep only removes candidates
older than 24, so a commit and the sweep never act on the same candidate.

Each ingestion also creates this bundle, which is what makes an outcome inspectable:

```text
ingested/<document-id>/<ingestion-id>/
  assets/       # copy of the source file
  metadata.json # extracted metadata
  status.json   # ok, needs_intervention, or failed
  *.duckdb      # spreadsheet database, when applicable
  manifest.json # artifact list, hashes, source, and outcome; uploaded last
```

Chunks are not written into the bundle - they live in the store, and the manifest names the
extractor and chunker that produced them and how many there are.

### Downstream interface

The ingestion task returns this completion value after the manifest is stored:

```json
{
  "event": "ingestion.completed",
  "schema_version": 1,
  "document_id": "uuid",
  "ingestion_id": "uuid",
  "pipeline_version": "1",
  "release": "release-1",
  "source_content_id": "hex-encoded SHA-256 - the document's identity",
  "status": "ok",
  "artifact_type": "chunks",
  "chunks": {"extractor": "tika@1", "chunker": "chunker@2", "count": 12},
  "extraction_reused": false,
  "chunks_reused": false,
  "manifest_uri": "s3://smart-files/ingested/<document-id>/<ingestion-id>/manifest.json",
  "completed_at": "2026-09-13T12:00:00+00:00"
}
```

The worker submits embedding only when the status is `ok` and chunks were produced. Immutable
bundle paths and each table's reuse key make retries safe: data is written in one transaction, so a
crash leaves nothing readable and the next attempt does the work again. Two runs that race converge
on one copy rather than one waiting on the other.

Each document row carries where the pipeline last got to:

| Field | Meaning |
|---|---|
| `status` | `uploaded`, `ingesting`, `embedding`, `ok`, `needs_intervention`, or `failed` |
| `error` | why a run failed or needs intervention, or `null` |
| `published` | `true` once the document is completely ingested and searchable |

Any flow may set `status`; the last write wins. It is for display only - nothing reads it to decide
what runs, so a worker killed outright can leave an in-progress value until the next run replaces
it. A crash Prefect sees is recorded as `failed` by the flows' `on_crashed` hooks. `published` stays
true once set, so a later run that fails leaves what was already built searchable, and search only
returns published documents. `GET /documents` returns all three fields, and `/library` shows them.

Every upload runs the pipeline again, including for bytes that are already a document: the
extraction, chunks, and embeddings are reused through their keys, while the bundle is redone.

### Why uploads go directly to S3

The browser asks the API for a presigned URL, `PUT`s the file straight to SeaweedFS, and then tells
the API the bytes have landed. The file never passes through the API process.

This keeps the API a control plane. An upload that went through it would hold a uvicorn worker for
as long as the transfer takes, so a handful of concurrent large files would make the API
unresponsive to requests that have nothing to do with uploading, and the API container would become
the bandwidth bottleneck and the scaling unit for ingest. Going direct also inherits multipart
uploads, resumption, and client-side retry from S3 instead of reimplementing them, and avoids
raising the body-size limit on every proxy in front of the API.

The cost is that the browser is the one place in the system that knows S3 exists. Below the HTTP
edge nothing does: tasks pass a `DocumentRef`, `src/store/objects.py` is the only place
object keys are built, and `S3_PUBLIC_ENDPOINT_URL` exists solely because a presigned signature is
bound to the host the browser will call, which is not the host the API uses.

A document *is* its bytes, so which document a file belongs to is unknown until it has been
hashed - and the client cannot be trusted to say. Rather than stage the bytes and copy them, the
server hands out a candidate `document_id` and the client writes straight to that candidate's
key. The URL is signed with `If-None-Match: *` and the declared `Content-Length`, so the key is
written once at exactly that size: the bytes that are hashed are the bytes the document keeps. A
candidate whose bytes are already a document is simply deleted.

Uploading direct does not, by itself, cost the client early feedback about duplicates: routing the
bytes through the API would not reveal a duplicate any sooner, because nothing can be hashed until
the last byte arrives either way. Early feedback comes from the client hashing the file itself and
calling `GET /documents/lookup` first, which is independent of where the bytes go and skips the
transfer entirely for a file that has been seen.

**When to reconsider.** If this only ever ingests documents small enough for the API to hold, an
upload endpoint would delete the presign step, the abandoned-upload sweep, and the second boto
client - three round trips become one. That trade is
worth taking for a proof of concept and stops being worth it as soon as file sizes are real.

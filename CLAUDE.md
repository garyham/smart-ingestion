# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`smart-files` is a proof-of-concept **document ingestion pipeline**. Source documents are uploaded
to SeaweedFS. Prefect background tasks queue work for long-lived workers. Structured output is
stored as immutable SeaweedFS/S3 bundles, using:

- **Prefect** for workflow orchestration (tasks/flows), with state stored in PostgreSQL.
- **Apache Tika Server** to detect MIME types and extract metadata and plain text.
- **langchain_text_splitters** to chunk the extracted text for onward use by an LLM.

What happens to the ingested output afterwards (retrieval, chat, vectorization/search) is **out of
scope** for this project — it only produces the ingested artifacts.

### Pipeline shape

For each source document, the ingestion flow may include:

- **Metadata extraction** — pulled from the document to (eventually, out of scope here) support
  vectorization/search.
- **Chunking** — optional, and not applicable to every document type.
- **Raw document storage** — the original file is preserved under an `assets/` folder.

Different document types are handled differently:

- Most document types are chunked for downstream LLM consumption.
- **`.xlsx` is a special case and is *not* chunked.** Instead, the spreadsheet is ingested into a
  DuckDB database and exposed as a queryable node — the LLM (downstream, out of scope here) queries it
  with SQL rather than reading chunks. Metadata is still extracted from the xlsx so it can be
  vectorised/queried like any other ingested document.

### Output layout

Ingestion output is uploaded under an immutable object prefix:

```
ingested/<document_id>/<ingestion_id>/
  assets/       # raw copy of the source document
  metadata.json # extracted metadata (for later vectorization/search)
  status.json   # ingestion outcome: ok / needs_intervention / failed (+ error details)
  *.duckdb      # (xlsx only) the spreadsheet ingested as a queryable DuckDB database
  manifest.json # uploaded last as the bundle completion marker
```

Chunks are **not** in the bundle. They are rows in the store's `chunks` table, served through
`DocumentStore.chunks()` / `get_chunks()`; the manifest names the extractor and chunker that
produced them and their count.
Source bytes live immutably in the document store, under `documents/<document_id>/`. Each
extractor version's MIME type and metadata are a row in the `extractions` table, and its text is
an object at `extractions/<document_id>/<extraction_id>.txt`.

After the manifest is stored, the ingestion task submits an embedding background task for a
successfully chunked document. Both stages are visible in Prefect.

Every run does the work again; what an operator version already produced is reused through
its table's key, not skipped by the flow. The document row carries a display-only `status`
(`uploaded`, `ingesting`, `embedding`, `ok`, `needs_intervention`, `failed`) with an `error`,
and a `published` flag that turns true once the document is completely ingested and searchable.
Completed artifacts are retained when embedding fails, and a published document stays published.

Test/sample input documents live in `data/`.

### Failure is a valid outcome

Ingestion can fail, or land in a state that requires user intervention (e.g. a document type or
layout the pipeline can't confidently parse). This is an expected, valid terminal state — not just an
unhandled error. Every ingestion run must write `status.json` into its bundle staging directory,
recording the outcome (`ok`, `needs_intervention`, or `failed`) and enough detail to explain why.
Partial output (e.g. a raw copy under `assets/`) should still be written where possible even when
ingestion doesn't fully succeed, so the failure is inspectable rather than silent.

## Current state of the code

Ingestion is built from one **store** and stateless, independently versioned **operators**. The
store (`src/store/`) owns every table, every object key, and the one Alembic chain. Extractors,
chunkers, and embedders (`src/operators/`) are plain classes that hold no connection and write nothing: a task
runs one, then hands what it produced to the store. A pipeline **release** only selects which
operator versions run. The orchestrator passes references - never a bucket, an object key, a
table, or a storage format.

The tables form one tree rooted at the document, every edge a cascading foreign key, so deleting a
document deletes everything derived from it:

```text
documents ──< extractions ──< chunks ──< embeddings
```

There are two layers and nothing in between. **Prefect flows and tasks control execution**:
sequencing, retries, concurrency, restarts. **The store writes rows and bytes to tables it owns**
and exposes each stage as a plain method a task can call. Operators compute and are called by
tasks, never by the store. Nothing holds a lease, counts attempts, or keeps an in-flight row.
Any flow may set a document's `status` through the store, but nothing reads it back to decide
what runs; `on_crashed` hooks record a crash as `failed`.

```text
API
  -> DocumentStore.create_upload()  -> UploadRef   (a candidate document_id; writes nothing)
  -> client PUTs bytes once to documents/<candidate>/ (write-once, size signed into the URL)
  -> /notify checks something arrived recently (a HEAD, no read) and queues process-upload
  -> DELETE /documents/{id} -> DocumentStore.delete_document() (rows + objects, one txn)
  -> /query -> embedder.embed_query() + embedder.search_options()
            -> DocumentStore.search_embeddings() -> [SearchHit]

@flow ingest-document
  commit_upload          @task  -> store.open_upload, SHA-256, age check
                                -> store.add_document -> DocumentRef (the bytes decide which;
                                   a duplicate candidate's bytes are discarded)
  find_extraction        @task  -> store.find_extraction  (reuse short-circuit)
  extract_document       @task  -> extractor.extract      (retries, holds a `tika` slot: the one
                                                           full parse - MIME type, text, metadata)
  store_extraction       @task  -> store.add_extraction -> ExtractionRef (text to S3, then the
                                   row; text never travels)
  route_document         @task  -> routing.route_mime_type, then calls the subflow
    @flow chunk-document         |  @flow ingest-xlsx (holds an `xlsx` slot; reads the metadata)
      find_chunks        @task  -> store.find_chunks      (reuse short-circuit)
      split_text         @task  -> store.extracted_text + chunker.split
      store_chunks       @task  -> store.add_chunks       (every chunk, one transaction)
  stage_bundle           @task
  publish_bundle         @task

@flow discard-abandoned-uploads  (served hourly by the `sweep` service)
                                -> store.discard_abandoned_uploads (candidates with no row,
                                   older than twice the commit window)

@flow embed-document
  find_pending_chunks    @task  -> store.find_extraction + store.unembedded_chunks (chunks this
                                   embedder has not embedded)
  encode_chunks          @task  -> embedder.embed          (retries: expensive/GPU)
  store_embeddings       @task  -> store.add_embeddings    (one transaction)
```

### Shared contracts

- `src/db.py` - the one declarative base, engine, and session factory, so Alembic sees a single
  metadata for the whole `smart_files` schema.
- `src/contracts/operators.py` - `OperatorRef` (`name@version`). An operator version is
  immutable: its configuration is part of what the version means.
- `src/contracts/refs.py` - the references the store returns: `UploadRef`, `DocumentRef`,
  `ExtractionRef`, `ChunksRef`, `EmbeddingsRef`, plus `ArtifactStatus` for the terminal outcomes a bundle's
  `status.json` records.
- `src/contracts/extractions.py` - `Extraction` (what an extractor produces: MIME type, text,
  metadata).
- `src/contracts/chunks.py` - `ChunkDraft` (what a chunker produces: no identity) and `Chunk`
  (what the store hands back once it has kept one).
- `src/contracts/embeddings.py` - `Embedding` / `SparseVector` (what an embedder produces),
  `SearchOptions` (the embedder version to search, the limit, and that version's own ranking
  settings), and `SearchHit` (a chunk, its document's name, and its score).

### Store

- `src/store/service.py` - `DocumentStore`. Persistence only: upload URLs, documents and
  their bytes, extractions, chunks, embeddings, and search. It does not hash or validate - the `commit-upload`
  task does that and hands the result to `add_document()` - and it never runs a model: `search_embeddings()`
  takes a query an embedder already embedded.
- `src/store/models.py` - the `documents`, `extractions`, `chunks`, and `embeddings` tables.
- `src/store/repository.py` - their persistence, including the transactional `delete_document`.
- `src/store/vectors.py` - writes and searches `embeddings` in raw SQL. Search joins `chunks`,
  `extractions`, and `documents` directly.
- `src/store/objects.py` - the only place document and extraction object keys are built.
- `src/store/schema.py` / `migrations/` - `ensure_schema()` and one Alembic chain for every
  table.

### Operators

- `src/operators/extractors/` - the `Extractor` protocol (bytes -> `Extraction`) and `tika@1`
  (`tika_v1.py`: one Tika `rmeta/text` parse). `extractors.get("tika@1")` looks a version up.
- `src/operators/chunkers/` - the `Chunker` protocol (text -> `ChunkDraft`s, nothing else) and
  `chunker@2` (`chunker_v2.py`: `langchain_text_splitters`). `chunkers.get("chunker@2")` looks a
  version up.
- `src/operators/embedders/` - the `Embedder` protocol and `hybrid@1` (`hybrid_v1.py`), which owns
  dense and sparse generation, query encoding, and its fusion settings as internals rather than
  as separate pipeline operators. `embedders.get("hybrid@1")` looks a version up.
- `src/releases.py` - the active release, read from `config/config.yaml`.

### Pipeline

- `src/ingestion/ingestion_flow.py` - the `ingest-document` flow (`commit-upload` through
  `publish-bundle`), the upload-event checks, and the `process-upload` queue entry point and
  its worker. The API submits ingestion with `.delay()`.
- `src/ingestion/chunking_flow.py` - the `chunk-document` subflow and its tasks.
- `src/ingestion/embedding_flow.py` - the `embed-document` flow, and the `embed-release` queue
  entry point and its worker.
- `src/ingestion/sweep_flow.py` - the served `discard-abandoned-uploads` flow.
- `src/ingestion/common.py` - what the flows share: `document_store()`, the retry policy, and
  recording a document's status, outcome, or crash.
- `src/ingestion/routing.py` - names the path a MIME type belongs to, and holds the global
  concurrency slots (`concurrency_slot`): `tika` bounds extraction, `xlsx` bounds DuckDB builds.
  It neither detects nor runs: the extraction carries the MIME type, and the flow's
  `route-document` task dispatches.
- `src/ingestion/xlsx.py` / `xlsx_flow.py` - the spreadsheet path, which never reaches a chunker
  because it produces a queryable DuckDB dataset rather than chunks.
- `src/ingestion/publish.py` - owns the `ingested/` bundle layout and writes the manifest last.
- `src/ingestion/assets.py` - `copy_to_assets`, `write_status`, `write_metadata`, and
  `stage_chunked`, which builds a chunked document's bundle from what chunking found.

### Reuse and identity

**A document is its content.** The same SHA-256 is the same document, so uploading identical bytes
twice yields the same `document_id` and every artifact derived from it is already built. There are
no document versions: different bytes are a different document. `documents.content_id` carries a
uniqueness constraint, and `document_id` is a generated surrogate behind it so references stay
short.

`/presign` therefore hands out only a **candidate** `document_id` and records nothing. The client
writes straight to that candidate's key, `documents/<candidate>/<filename>`, through a URL signed
for one write (`If-None-Match: *`) of exactly the declared size, so the bytes that get hashed are
the bytes the document keeps. `/notify` only checks that something arrived and queues the flow.
The ingestion flow's `commit-upload` task hashes the bytes; `add_document` inserts the row under
the candidate's ID, or - when the bytes are already a document - returns that document and
deletes the candidate's bytes. A candidate never gets a row until it is known to be new, so
`content_id` is never null. Candidates that never become documents are removed by the
`discard-abandoned-uploads` flow; `commit-upload` refuses bytes older than `UPLOAD_COMMIT_WINDOW`
and the sweep only removes candidates twice that old, so the two never act on the same
candidate. A client that hashes its own bytes can ask `/documents/lookup` whether they are
already a document and skip the upload, but that hash is unverified - it answers the existence
question and grants nothing.

An operator result is reusable when its **operator version** and **input identity** match.
Operator versions are immutable, so there is no configuration hash. `pipeline_version` is
deliberately not part of that key. Input identity is just the document or chunk ID - deduplication
already happened at the document layer, so nothing downstream needs to reason about content hashes.

There are no set or registration tables. The data rows carry the operator that produced them, and
each table's key is the reuse rule: `extractions_reuse_key` is `(document_id, extractor_name,
extractor_version)`, `chunks_reuse_key` is `(extraction_id, chunker_name, chunker_version,
chunk_index)` - chunks are cut from one extraction's text - and the `embeddings` primary key is
`(chunk_id, embedder_name, embedder_version)`. There is no lease and no claim: an extraction row,
a document's chunks, and each batch of embeddings are inserted in one transaction with
`ON CONFLICT DO NOTHING`. A run that dies leaves nothing readable, so the next attempt simply
does the work again, and two runs that race converge on one copy. An extraction's text is written
before its row, under the run's own `extraction_id`, so racing runs never share a key: the run
whose insert loses deletes its text and takes the winner's reference. Failures are recorded by Prefect and the document's `status`, never by a claim.

There is no downstream Q&A/chat loop in this codebase — that concern is out of scope per the
invariants below, and ingestion stops once metadata, (optional) chunks, and (for xlsx) the queryable
DuckDB file are published as an S3 bundle.

## Commands

This project uses `uv` for dependency management (Python >=3.12, deps pinned in `uv.lock`).

```bash
# install dependencies
uv sync

# run the tests and the linter
uv run pytest
uv run ruff check src tests

# build and start PostgreSQL, SeaweedFS, Prefect, the API, and the worker
docker compose up -d --build --wait

# stop all services
docker compose down
```

Open `http://127.0.0.1:8000` to upload a file.

`tests/test_reuse.py` exercises the reuse rule - including two simultaneous identical builds
converging on one copy - and the delete cascade against a real PostgreSQL, and skips itself when none is reachable. Every
other test runs offline; `tests/test_flows.py` runs the flows against
`prefect_test_harness()`. Start the database with
`docker compose up -d smart-files-postgres` to include it.

### Key invariants to preserve when building the ingestion pipeline

- Ingestion output goes to immutable S3 bundles; input test documents come from `data/`.
- Ingestion is the full scope of this project — do not build in downstream consumption (chat,
  retrieval, vector search) as part of this pipeline.
- `.xlsx` documents are ingested as a queryable DuckDB database, not chunked; other document types are
  converted to plain text by Apache Tika and chunked via
  `langchain_text_splitters`.
- Every document type still gets metadata extraction and raw-file storage under `assets/`, regardless
  of whether it's chunked or exposed as a DuckDB node.
- Ingestion failure / needs-intervention is a valid, expected outcome, not just an error to bubble up —
  every bundle must contain a `status.json` reflecting what actually happened, and the
  document's `status` must record the same outcome.
- The store owns every table, object key, and storage format; nothing else reads or writes them.
  Operators never touch storage, and the store never runs an operator - a task runs the operator
  and hands its output to the store. Pass references, never keys or tables.
- Every table derived from a document must reach it through a cascading foreign key, and every
  object a document owns must be removed by `delete_document`, so deleting a document leaves
  nothing behind.
- A release selects operator versions and nothing more. Never put `pipeline_version` or a release
  name into an operator's reuse key. Operator versions are immutable: a changed configuration is a
  new version.
- Write an operator's data in one transaction, so an interrupted run leaves nothing readable
  rather than a half-built artifact. Retries and sequencing belong to Prefect: never reintroduce a
  lease, an attempt counter, or a set/registration table into the store. `documents.status` is
  for display only: never read it to decide whether work runs, is skipped, or is retried.
- Search returns only published documents.
- Search must name an embedder version or a release. It must never sweep every embedding.
- A document is identified by the SHA-256 of its bytes and nothing else. Do not reintroduce
  document versions, and do not let a caller choose which document an upload becomes: the
  server assigns the candidate ID and the hash decides whether it survives. Do not reintroduce
  an upload-session table or a staging copy; keep the upload URL write-once.

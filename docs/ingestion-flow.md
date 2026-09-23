# Ingestion flow: API to published

This follows one document from upload to `published = true` and lists what is written at each
stage. There are four places things are kept:

- **PG**: the `smart_files` PostgreSQL schema
- **S3**: SeaweedFS
- **tmp**: the worker's local temporary directory
- **Prefect**: Prefect's own database (flow and task run state)

---

## 1. `POST /presign`: hand out a candidate document id

`api.py:presign` → `DocumentStore.create_upload`

| Where | What's written |
|---|---|
| PG / S3 | Nothing |

The store picks a candidate `document_id` (a uuid4) and presigns a PUT to
`documents/<document_id>/<safe_filename>`. The signature covers `Content-Type`,
`Content-Length` (the declared size, which must be at least 1) and `If-None-Match: *`. So the
key can be written once, at exactly that size, and never again. The client gets back
`document_id`, `upload_url`, `upload_headers` (which it must send exactly) and `expires_at`
(15 minutes).

The candidate is not a document yet. It becomes one only if its bytes turn out to be new.

*Optional shortcut:* `GET /documents/lookup?sha256=…` only reads. It tells a client that has
hashed its own file whether to bother uploading.

## 2. Client PUTs the bytes

The browser uploads directly to SeaweedFS. The API isn't involved.

| Where | What's written |
|---|---|
| S3 | `documents/<document_id>/<filename>`. This is the document's final key, but it has no row yet. |

SeaweedFS answers a second PUT to the same key with `412`, and a body of the wrong length with
`403`.

## 3. `POST /notify`: cheap checks, then queue

`api.py:notify`. The bytes are not read here.

1. If a document row already exists with this id, the checks are skipped and the event is queued
   again.
2. Otherwise `describe_upload` lists the candidate's prefix and sends a `HEAD`. No object returns
   `404`. `check_upload` rejects an empty object, or one older than `UPLOAD_COMMIT_WINDOW`
   (12 h), with `400`.
3. It builds an `upload.notified` event (schema 4): `event_id` (a new uuid4), `document_id` (the
   candidate) and the active `release` name. It then calls `process_upload.delay(event)`.

| Where | What's written |
|---|---|
| Prefect | A scheduled `process-upload` task run holding the event as its parameter |
| PG / S3 | Nothing |

The response is `202` with `event_id`, `document_id` (still only a candidate) and `task_run_id`.

## 4. `process-upload` task (ingest worker, `limit=1`)

`ingestion/ingestion_flow.py:process_upload`. It validates the event, then runs the `ingest-document`
flow. `InvalidUploadEvent` and `InvalidUpload` are never retried.

### 4a. `ingest-document` flow

#### `commit-upload` task (retries 3): hash the bytes and resolve them to a document

1. If a document row already exists with the candidate's id, it returns that `DocumentRef`.
2. It runs `check_upload` again, then streams the candidate's object and computes SHA-256 and
   the size. Nothing under the candidate's prefix is `InvalidUpload`. That includes a duplicate
   whose bytes were already discarded, so it isn't retried.
3. `store.add_document(document_id, content_id=sha, size)` inserts the row under the
   candidate's id with `ON CONFLICT (content_id) DO NOTHING`, then reads back whichever row
   holds that hash:
   - **The bytes are new:** PG `documents` gets a new row with `document_id=<candidate>`,
     `content_id`, `object_key=documents/<candidate>/<filename>`, `status='uploaded'` and
     `published=false`. S3 is untouched, because the bytes are already at their key.
   - **The bytes are already a document** (including a concurrent upload of the same bytes
     that won the insert): no row is written, the existing `DocumentRef` is returned, and S3
     deletes `documents/<candidate>/`. The delete is best effort; if it fails, the sweep
     removes it later.

If this task fails, there is no document to record a status on. The failure is visible only in
Prefect, and the candidate's bytes stay until the sweep removes them.

#### After the commit

| Step | Where | What's written |
|---|---|---|
| `record_status(INGESTING)` | PG `documents.status` | `ingesting` |
| `store.download()` | tmp | `<tmp>/<safe_filename>`, a local copy of `documents/<id>/…` |
| `find-extraction` task → `store.find_extraction` | reads PG `extractions` by `(document_id, extractor@ver)` | If the release's extractor version already extracted this document, it returns `ExtractionRef(reused=True)` and skips the next two tasks |
| `extract-document` task (retries 3): `extractor.extract`, one Tika `rmeta/text` parse | Prefect result | `Extraction`: MIME type, text, and a metadata dict with `character_count`. Holds a slot in the `tika-ingest` global concurrency limit while it runs |
| `store-extraction` task → `store.add_extraction` | S3 `extractions/<id>/<extraction_id>.txt`, then the PG `extractions` row (`mime_type`, `metadata`) last, `ON CONFLICT (extractions_reuse_key) DO NOTHING` | An `ExtractionRef` (extraction, document, extractor, MIME type). A run whose insert loses a race deletes its own text and returns the winner's reference. From here on only the reference travels, never the text |
| `route-document` task | Prefect | `route_mime_type` picks `chunks` or `duckdb` from the extraction's MIME type and calls that path's subflow |

### 4b-i. Chunked path: `chunk-document` subflow

| Task | Where | What's written |
|---|---|---|
| `find-chunks` | reads PG `chunks` | If rows exist for `(extraction_id, chunker@ver)`, it returns `ChunksRef(reused=True)` and skips the next two tasks |
| `split-text`: `store.extracted_text` + `chunker.split` | Prefect result | A list of `ChunkDraft`. If the text yields none, the flow returns `NEEDS_INTERVENTION` (`no_content_extracted`) and writes no chunks |
| `store-chunks` → `store.add_chunks` | PG `chunks` | Every row in **one transaction**, `ON CONFLICT (chunks_reuse_key) DO NOTHING`. Columns: `chunk_id`, `extraction_id`, `chunker_name/version`, `chunk_index`, `headings`, `text` |

Back in the parent flow, the `stage-bundle` task calls `stage_chunked` and writes to tmp at
`output/<stem>/`:

- `assets/<filename>`: the raw copy
- `metadata.json`: the extraction's metadata plus `extractor`, `chunker` and `chunk_count`
- `status.json`: `{"status": "ok" | "needs_intervention", …reason}`

### 4b-ii. xlsx path: `ingest-xlsx` subflow

It runs holding a slot in the `xlsx-ingest` global concurrency limit, and reads the extraction's
metadata from the store rather than parsing the file again. Everything goes to tmp under `output/<stem>/`:

- `assets/<filename>`
- `<stem>.duckdb`: the spreadsheet loaded as queryable tables
- `metadata.json`: the extraction's metadata plus `extractor`, `kind=queryable_dataset`, `tables`
  and `background`
- `status.json`: `ok`, or `needs_intervention` (`no_data_tables_found` / `some_sheets_failed`),
  or `failed` (`xlsx_ingestion_error`)

Nothing is written to PG `chunks` on this path.

### 4c. Publish the bundle

The flow first asserts that `status.json` exists, then runs the `publish-bundle` task
(`ingestion/publish.py`):

| Where | What's written |
|---|---|
| S3 `ingested/<document_id>/<event_id>/…` | Every staged file: `assets/…`, `metadata.json`, `status.json`, and `*.duckdb` for xlsx |
| S3 `…/manifest.json` | **Last**, as the completion marker. It holds `schema_version`, `pipeline_version` (provenance only), `created_at`, `status`, `artifact_type`, `source` (`document_id`, `content_id`, `filename`, as a reference, not a key), `chunks` (`{extractor, chunker, count}` or null), and `artifacts[]` (name, URI, content type, size, sha256) |

Note that `ingestion_id` is the `event_id`, so every `/notify` produces a new bundle prefix.

### 4d. `record_outcome`: carry `status.json` onto the document

| Outcome | PG `documents` |
|---|---|
| `needs_intervention` / `failed` | `status=<that>`, `error=<reason/detail>`, `published` unchanged |
| `ok` with chunks | `status='embedding'` |
| `ok` without chunks (xlsx) | `mark_published`: `status='ok'`, `error=NULL`, **`published=true`**. This is the end of the line for xlsx. |

If an exception escapes the flow after the commit, it records `status='failed'` with
`{type, detail}` and re-raises, so Prefect sees the failure. A hard crash is recorded as
`failed` by the `on_crashed` hook instead. The hook records against the candidate's id, which does
nothing if the candidate never got a row. A crash while processing a duplicate is therefore
not recorded on the existing document.

The tmp directory is deleted when the flow exits.

### 4e. Hand off to embedding

If `completion.status == "ok"` and chunks exist, `process_upload` calls
`embed_release.delay(document_id, release)`.

| Where | What's written |
|---|---|
| Prefect | A scheduled `embed-release` task run. Its id is returned in the completion. |

## 5. `embed-release` task (embedding worker, `limit=1`)

First it calls `record_status(EMBEDDING)`. Then, for each embedder the release names, it runs
the `embed-document` subflow:

| Task | Where | What's written |
|---|---|---|
| `find-pending-chunks` | reads PG `extractions`, then `chunks` left-joined to `embeddings` | Finds the release extractor's extraction, then returns the chunk count and the chunks this embedder version hasn't embedded yet |
| `encode-chunks` (retries 2): `embedder.embed(texts)` | Prefect result | Dense and sparse vectors |
| `store-embeddings` → `store.add_embeddings` | PG `embeddings` | One transaction, keyed on `(chunk_id, embedder_name, embedder_version)`: `dense_embedding`, `sparse_embedding` |

A retry therefore only encodes whatever the previous attempt didn't commit.

When every embedder has run, `mark_published` sets PG `documents.status='ok'`, `error=NULL` and
**`published=true`**. The document can now be searched: `/query` → `vectors.search` only returns
published documents.

If embedding fails, the document gets `status='failed'` with the error. `published` is not
reset, the chunks and bundle stay, and so do the embeddings of any batches that committed.

---

## 6. `discard-abandoned-uploads` flow (the `sweep` service, hourly)

`ingestion/sweep_flow.py:discard_abandoned_uploads_flow` → `store.discard_abandoned_uploads`. It
lists `documents/`, groups the keys by candidate, and deletes every candidate whose newest
object is older than `ABANDONED_AFTER` (24 h) and which has no document row. That covers bytes
from a client that never called `/notify`, and duplicates whose delete failed.

`commit-upload` refuses bytes older than 12 h and the sweep only removes candidates older than
24 h. So the sweep never deletes bytes a commit is about to record, unless a single commit takes
longer than 12 h.

---

## What exists at the end

```text
S3   documents/<document_id>/<filename>                    source bytes, immutable
     extractions/<document_id>/<extraction_id>.txt         extracted text
     ingested/<document_id>/<event_id>/assets/<filename>
                                      /metadata.json
                                      /status.json
                                      /<stem>.duckdb       (xlsx only)
                                      /manifest.json       (written last)
PG   documents   1 row, status=ok, published=true
     extractions 1 row for (document, extractor@v): MIME type + metadata
     chunks      N rows for (extraction, chunker@v)        (chunked only)
     embeddings  N × embedders in the release              (chunked only)
```

## Open observations

1. **A retried `process-upload` re-publishes to the same bundle prefix.** The ingestion id is
   the `event_id`, and that stays the same across the task's 4 retries. If the task fails after
   `publish-bundle`, for example because `embed_release.delay` can't reach Prefect, the retry
   rewrites the same keys with a new manifest `created_at`. That conflicts with "immutable
   bundle". A retry of `publish-bundle` itself has the same effect, though it writes identical
   content apart from the timestamp.
2. **"Published" means different things for xlsx.** An xlsx document becomes `published=true`
   straight after the bundle, but it has no chunks or embeddings, so search can never return
   it. That's consistent with the scope (DuckDB is queried downstream), but "published =
   searchable" in `CLAUDE.md` only holds for chunked documents.

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`smart-files` is a proof-of-concept **document ingestion pipeline**. Source documents are dropped
into a `queue/` folder; a Celery Beat-scheduled poll task polls on a cron schedule, ingests new
arrivals, and writes structured output to `ingested/`, using:

- **Celery** (with Redis as broker + result backend) for task scheduling/execution, with
  per-document-type queues and worker pools for concurrency control.
- **pymupdf4llm** to convert PDFs to markdown, and **markitdown** to convert other document types
  (Word/ODT, HTML, CSV, plain text, email) to markdown.
- **langchain_text_splitters** to chunk the converted markdown for onward use by an LLM.

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

Ingestion output is written to `ingested/`, one folder per source document:

```
ingested/<doc_name>/
  assets/       # raw copy of the source document
  metadata      # extracted metadata (for later vectorization/search)
  status.json   # ingestion outcome: ok / needs_intervention / failed (+ error details)
  *.duckdb      # (xlsx only) the spreadsheet ingested as a queryable DuckDB database
```

Test/sample input documents live in `data/` (e.g.
`data/UK_armed_forces_equipment_and_formations_2025.xlsx`) — copy one into `queue/` to have it
picked up and ingested.

### Failure is a valid outcome

Ingestion can fail, or land in a state that requires user intervention (e.g. a document type or
layout the pipeline can't confidently parse). This is an expected, valid terminal state — not just an
unhandled error. Every ingestion run must write `status.json` into the document's `ingested/<doc_name>/`
folder recording the outcome (`ok`, `needs_intervention`, or `failed`) and enough detail to explain why.
Partial output (e.g. a raw copy under `assets/`) should still be written where possible even when
ingestion doesn't fully succeed, so the failure is inspectable rather than silent.

## Current state of the code

The pipeline described above is implemented, laid out as a `src/` package. `src/celery_app.py`
defines the Celery app (Redis broker + result backend, per-task-type queue routing, and the
`beat_schedule` that replaces cron polling), and `src/tasks.py` holds all the Celery tasks:

- `poll_and_enqueue_task` — Beat-triggered every minute. Scans the `queue/` landing folder for
  files, records them in a SQLite queue table (`state/queue.db`, managed by `src/queue_db.py` —
  tracks `pending`/`processing`/`success`/`failed` per document URI; a URI already
  `pending`/`processing` is left alone so an in-flight document isn't queued twice, but any other
  prior status is reset to `pending` on rescan, so dropping a previously-processed file back into
  `queue/` re-ingests it regardless of past outcome), then fires off `dispatch_item_task` for every
  pending row without waiting on any of them — Celery's chain continuation (see below) means no
  join step is needed.
- `dispatch_item_task` — detects a document's MIME type (`src/ingestion/detect.py`) and classifies
  it (`src/ingestion/routing.py`'s `classify_document`) into `pdf`/`xlsx`/`markitdown`/`unsupported`,
  then chains the matching ingestion task to `finalize_task` and fires it off.
- `ingest_pdf_task` / `ingest_markitdown_task` / `ingest_xlsx_task` / `mark_unsupported_task` — each
  routed to its own Celery queue (`pdf-ingest`, `markitdown-ingest`, `xlsx-ingest`, `default`).
  Concurrency is capped per document type by starting each queue's worker process group with a
  fixed `--concurrency` (values from `config/config.yaml`'s `concurrency_limits`, read by
  `smart_files_ctl` at startup) — e.g. at most N PDF conversions run at once regardless of how many
  documents are queued overall, enforced by the OS-level worker pool rather than a provisioned
  server-side limit.
- `finalize_task` — chained after every ingestion task; records `success`/`failed` in `queue_db` and
  removes the file from `queue/` (the raw copy is preserved separately under
  `ingested/<doc>/assets/` regardless of outcome).

Each per-type ingestion task wraps a plain business-logic function that does convert-then-chunk (or
xlsx-extract) as two sequential calls in one task body — not a further chain — so a worker's
`--concurrency` on a typed queue caps full-pipeline concurrency for that type, not just the
conversion step. Each of these functions has its own top-level `try/except Exception` safety net so
an unexpected failure can never strand a queue row in `processing` forever. Scheduling requires
Redis plus the Celery workers/beat to be running (`./smart_files_ctl start`), which fails fast if
the observability API doesn't come up healthy. A small read-only FastAPI app
(`src/observability_api.py`, port 8100) exposes document/queue-row state for inspection (the
document-domain view Prefect's UI didn't have an equivalent of either), alongside Flower
(`celery -A celery_app flower`, port 5555) for Celery's own task/worker-level view (live worker
status, task history/retries, per-queue depths). The per-concern logic lives under
`src/ingestion/`:

- `src/ingestion/config.py` — loads `config/config.yaml` (MIME whitelist, concurrency limits).
- `src/ingestion/detect.py` — MIME detection (`DetectedType`, `identify_mime_type`).
- `src/ingestion/routing.py` — `classify_document`, mapping a detected MIME type to an ingestion
  path, and `mark_unsupported` for anything outside the whitelist; used by `dispatch_item_task`.
- `src/ingestion/outcome.py` — `read_outcome`, reads a document's `status.json` back into a
  `(succeeded, error_detail)` tuple; shared by every ingestion task.
- `src/ingestion/assets.py` — shared output-writing helpers (`copy_to_assets`, `write_status`,
  `write_metadata`) used by every ingestion path.
- `src/ingestion/pdf_ingest.py` — converts PDFs to markdown via pymupdf4llm (`convert_pdf`), writing
  assets/metadata, then chunks; `run_pdf_ingest` wraps the two steps.
- `src/ingestion/markitdown_ingest.py` — converts other non-xlsx document types to markdown via
  markitdown (`convert_with_markitdown`), writing assets/metadata, then chunks;
  `run_markitdown_ingest` wraps the two steps.
- `src/ingestion/chunking.py` — shared chunking step (`chunk_and_finalize`): splits converted markdown
  via `langchain_text_splitters` (header-aware split, then size-capped) and writes `chunks.jsonl` +
  the final `status.json` for non-xlsx types.
- `src/ingestion/xlsx.py` — pure DuckDB library for xlsx ingestion (no orchestration-framework
  dependency): locates numeric/data sheets vs. metadata sheets (`Contents`, `Background
  Information`, `Notes`), maps sheet numbers to titles via the `Contents` sheet's
  `Worksheet <n> – <title>` format, detects header rows, slugifies column names, infers
  `BIGINT`/`DOUBLE`/`VARCHAR` column types, and computes per-column summary stats.
- `src/ingestion/xlsx_ingest.py` — `run_xlsx_ingest`, wrapping `src/ingestion/xlsx.py` to persist
  the DuckDB file and write `metadata`/`status.json`.

There is no downstream Q&A/chat loop in this codebase — that concern is out of scope per the
invariants below, and ingestion stops once metadata, (optional) chunks, and (for xlsx) the queryable
DuckDB file are written to `ingested/<doc_name>/`.

## Commands

This project uses `uv` for dependency management (Python >=3.12, deps pinned in `uv.lock`).

```bash
# install dependencies
uv sync

# start everything: redis, the per-document-type Celery workers, Celery beat (the cron
# scheduler), Flower (Celery's monitoring UI, http://127.0.0.1:5555), and the read-only
# observability API (http://127.0.0.1:8100)
./smart_files_ctl start

# stop everything smart_files_ctl started
./smart_files_ctl stop
```

Drop a file into `queue/` and it'll be picked up (and recorded in `state/queue.db`) on the next
poll (every minute). `queue/` and `state/` (which now also holds worker/beat PID files and logs
under `state/pids/` and `state/logs/`) are gitignored, local-only working directories.

There is no lint/test/build tooling configured yet.

### Key invariants to preserve when building the ingestion pipeline

- Ingestion output goes to `ingested/`; input test documents come from `data/`.
- Ingestion is the full scope of this project — do not build in downstream consumption (chat,
  retrieval, vector search) as part of this pipeline.
- `.xlsx` documents are ingested as a queryable DuckDB database, not chunked; other document types are
  converted to markdown (pymupdf4llm for PDF, markitdown for everything else) and chunked via
  `langchain_text_splitters`.
- Every document type still gets metadata extraction and raw-file storage under `assets/`, regardless
  of whether it's chunked or exposed as a DuckDB node.
- Ingestion failure / needs-intervention is a valid, expected outcome, not just an error to bubble up —
  every document's output folder must end up with a `status.json` reflecting what actually happened.

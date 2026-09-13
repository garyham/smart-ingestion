# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`smart-files` is a proof-of-concept **document ingestion pipeline**. Source documents are uploaded
to SeaweedFS. RabbitMQ notifies the worker, which downloads and ingests each new file. Structured
output is stored as immutable SeaweedFS/S3 bundles, using:

- **Prefect** for workflow orchestration (tasks/flows), with state stored in PostgreSQL.
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

Ingestion output is uploaded under an immutable object prefix:

```
ingested/<document_id>/<ingestion_id>/
  assets/       # raw copy of the source document
  metadata.json # extracted metadata (for later vectorization/search)
  status.json   # ingestion outcome: ok / needs_intervention / failed (+ error details)
  *.duckdb      # (xlsx only) the spreadsheet ingested as a queryable DuckDB database
  manifest.json # uploaded last as the bundle completion marker
```

After the manifest is stored, the worker publishes `ingestion.completed` to the durable
`file-processing` RabbitMQ queue. Consumers fetch the bundle through its `manifest_uri` and
deduplicate retries by `ingestion_id`.

Test/sample input documents live in `data/`.

### Failure is a valid outcome

Ingestion can fail, or land in a state that requires user intervention (e.g. a document type or
layout the pipeline can't confidently parse). This is an expected, valid terminal state — not just an
unhandled error. Every ingestion run must write `status.json` into its bundle staging directory,
recording the outcome (`ok`, `needs_intervention`, or `failed`) and enough detail to explain why.
Partial output (e.g. a raw copy under `assets/`) should still be written where possible even when
ingestion doesn't fully succeed, so the failure is inspectable rather than silent.

## Current state of the code

The pipeline is implemented as a `src/` package. `src/poll_ingest.py` consumes the durable
`file-ingestion` queue, which is bound to the `file-uploads` fanout exchange. It validates each
event, downloads the object from SeaweedFS, and routes it through the correct ingestion subflow.
It acknowledges work after the bundle and completion event are published, rejects invalid events,
and requeues infrastructure or unhandled errors. Prefect
global concurrency limits are configured in `config/config.yaml`. The per-concern logic lives
under `src/ingestion/`:

- `src/ingestion/config.py` — loads `config/config.yaml` (MIME whitelist).
- `src/ingestion/detect.py` — MIME detection (`DetectedType`, `identify_mime_type`).
- `src/ingestion/routing.py` — `route_document`, dispatching a detected document to the right
  ingestion subflow (pdf/markitdown/xlsx) or marking it unsupported; shared by `poll_ingest.py`.
- `src/ingestion/assets.py` — shared staging helpers (`copy_to_assets`, `write_status`,
  `write_metadata`) used by every ingestion path.
- `src/ingestion/publish.py` — uploads artifacts, calculates hashes, and writes the manifest last.
- `src/ingestion/pdf_ingest.py` — converts PDFs to markdown via pymupdf4llm (`convert_pdf`), writing
  assets/metadata, then chunks; `pdf_ingest_flow` is the subflow wrapping the two steps.
- `src/ingestion/markitdown_ingest.py` — converts other non-xlsx document types to markdown via
  markitdown (`convert_with_markitdown`), writing assets/metadata, then chunks;
  `markitdown_ingest_flow` is the subflow wrapping the two steps.
- `src/ingestion/chunking.py` — shared chunking step (`chunk_and_finalize`): splits converted markdown
  via `langchain_text_splitters` (header-aware split, then size-capped) and writes `chunks.jsonl` +
  the final `status.json` for non-xlsx types.
- `src/ingestion/xlsx.py` — pure DuckDB library for xlsx ingestion (no Prefect dependency): locates
  numeric/data sheets vs. metadata sheets (`Contents`, `Background Information`, `Notes`), maps sheet
  numbers to titles via the `Contents` sheet's `Worksheet <n> – <title>` format, detects header rows,
  slugifies column names, infers `BIGINT`/`DOUBLE`/`VARCHAR` column types, and computes per-column
  summary stats.
- `src/ingestion/xlsx_ingest.py` — `xlsx_ingest_flow`, the Prefect subflow wrapping
  `src/ingestion/xlsx.py` that persists the DuckDB file and writes `metadata.json`/`status.json`.

There is no downstream Q&A/chat loop in this codebase — that concern is out of scope per the
invariants below, and ingestion stops once metadata, (optional) chunks, and (for xlsx) the queryable
DuckDB file are published as an S3 bundle.

## Commands

This project uses `uv` for dependency management (Python >=3.12, deps pinned in `uv.lock`).

```bash
# install dependencies
uv sync

# build and start PostgreSQL, SeaweedFS, RabbitMQ, Prefect, the API, and the worker
docker compose up -d --build --wait

# stop all services
docker compose down
```

Open `http://127.0.0.1:8000` to upload a file.

There is no lint/test/build tooling configured yet.

### Key invariants to preserve when building the ingestion pipeline

- Ingestion output goes to immutable S3 bundles; input test documents come from `data/`.
- Ingestion is the full scope of this project — do not build in downstream consumption (chat,
  retrieval, vector search) as part of this pipeline.
- `.xlsx` documents are ingested as a queryable DuckDB database, not chunked; other document types are
  converted to markdown (pymupdf4llm for PDF, markitdown for everything else) and chunked via
  `langchain_text_splitters`.
- Every document type still gets metadata extraction and raw-file storage under `assets/`, regardless
  of whether it's chunked or exposed as a DuckDB node.
- Ingestion failure / needs-intervention is a valid, expected outcome, not just an error to bubble up —
  every bundle must contain a `status.json` reflecting what actually happened.

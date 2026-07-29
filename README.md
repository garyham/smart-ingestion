# smart-files

A proof-of-concept document ingestion pipeline: drop source documents into `queue/`, a Celery
Beat-scheduled poll task polls on a cron schedule and ingests new arrivals into `ingested/`
(metadata, chunks, and for `.xlsx` a queryable DuckDB database).

## Installation

Requires Python >=3.12, [`uv`](https://docs.astral.sh/uv/), and a local `redis-server`/`redis-cli`
install (used as the Celery broker + result backend).

```bash
# clone the repo, then from its root:
uv sync
```

This installs the pinned dependencies from `uv.lock` (Celery, pymupdf4llm, markitdown, duckdb,
langchain_text_splitters, fastapi, etc.) into a local `.venv`.

## Operation

```bash
# start everything: redis, the per-document-type Celery workers, Celery beat, and the
# read-only observability API
./smart_files_ctl start
```

With everything running, drop a document into `queue/` and it's picked up on the next poll (every
minute):

```bash
cp data/UK_armed_forces_equipment_and_formations_2025.xlsx queue/
# a minute or so later:
ls ingested/UK_armed_forces_equipment_and_formations_2025/
# assets/  metadata  status.json  UK_armed_forces_equipment_and_formations_2025.duckdb
```

A PDF or Word doc ingests the same way, but is chunked instead of turned into a DuckDB database:

```bash
cp data/Armed_Forces_Covenant_annual_report_summary_2025.pdf queue/
# a minute or so later:
ls ingested/Armed_Forces_Covenant_annual_report_summary_2025/
# assets/  chunks.jsonl  metadata  status.json
```

Check `status.json` in a document's output folder for the ingestion outcome (`ok`,
`needs_intervention`, or `failed`) and, on failure, why.

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

Spreadsheet types (Excel/ODS) are ingested into a `.duckdb` file under
`ingested/<doc_name>/` and queried with SQL downstream instead of being split into chunks.

Other useful commands:

```bash
# stop everything smart_files_ctl started (redis, workers, beat, observability API)
./smart_files_ctl stop

# read-only observability API, for inspecting queue/document state directly
curl http://127.0.0.1:8100/queue
curl http://127.0.0.1:8100/documents/<doc_name>
```

`queue/` and `state/` (the SQLite queue tracking what's been seen/processed, plus worker/beat PID
files and logs) are gitignored, local-only working directories.

## How it works

### Pipeline architecture

```
Beat, every minute
  -> tasks.poll_and_enqueue_task            [queue: default]
       scans queue/, records new arrivals in state/queue.db,
       fires tasks.dispatch_item_task for every pending row (no waiting on the result)

tasks.dispatch_item_task(item_id, uri)      [queue: default]
  detects the file's MIME type and classifies it as pdf / xlsx / markitdown / unsupported,
  then chains the matching ingest task -> tasks.finalize_task and fires it off

tasks.ingest_pdf_task / ingest_markitdown_task / ingest_xlsx_task / mark_unsupported_task
  [queue: pdf-ingest / markitdown-ingest / xlsx-ingest / default respectively]
  does the actual conversion + chunking (or DuckDB extraction), writes metadata/status.json

tasks.finalize_task(outcome, item_id, uri)  [queue: default]
  records success/failed in state/queue.db and removes the file from queue/
```

Each document's full pipeline (convert + chunk, or the xlsx extract) runs as a single task rather
than several chained steps, so a queue's worker `--concurrency` caps how many *whole documents* of
that type are being processed at once - not just the conversion step.

### Celery, just the relevant bits

This project's use of Celery is fairly narrow. The concepts that actually matter here:

- **Task** - a plain Python function decorated with `@app.task` (see `src/tasks.py`). Calling
  `some_task.apply_async(...)` (or `.delay(...)`) doesn't run the function inline - it sends a
  message describing the call to the broker, and returns immediately. Some *worker* process picks
  that message up later and actually runs it.
- **Broker** - the message queue tasks are sent to and consumed from. This project uses Redis for
  it (`CELERY_BROKER_URL` in `src/celery_app.py`, defaulting to a local Redis). Redis is also used
  as the **result backend** here - where a task's return value is stored for a short while
  (`result_expires` in `celery_app.py`) so it can be picked up by whatever comes next.
- **Queue + routing** - a named channel on the broker that tasks get routed to and workers consume
  from. By default every task goes to one queue called `celery`; `task_routes` in `celery_app.py`
  instead sends each task to a queue matching its document type (`pdf-ingest`,
  `markitdown-ingest`, `xlsx-ingest`, or `default` for the lightweight scan/dispatch/finalize
  work). This is what lets PDF-heavy work be scaled/capped independently of everything else.
- **Worker** - a process that connects to the broker and executes tasks from one or more queues
  (`celery -A celery_app worker -Q <queue> --concurrency=N ...`, see `smart_files_ctl`). `-Q`
  picks which queue(s) it drains; `--concurrency=N` is how many tasks it runs in parallel. This
  project uses `--pool=prefork` (Celery's default), meaning each of those N slots is a separate OS
  process rather than a thread - the right choice here since PDF/markitdown conversion is
  CPU-bound native code that wouldn't parallelize well under Python's GIL with threads. One worker
  process group per typed queue, each started with its own `--concurrency` (read from
  `config/config.yaml`'s `concurrency_limits`), is the entire mechanism behind "at most N PDFs
  converting at once."
- **Beat** - a separate scheduler process (`celery -A celery_app beat`) that runs no task logic
  itself. On the cron-like schedule in `beat_schedule` (`celery_app.py`), it just enqueues
  `poll_and_enqueue_task` for a worker to pick up - this is the direct replacement for what
  Prefect's `poll_and_ingest_flow.serve(cron=...)` used to do.
- **Chain** - `chain(taskA.s(...), taskB.s(...)).apply_async()` links two tasks so `taskB`
  automatically runs once `taskA` finishes, with `taskA`'s return value passed in as `taskB`'s
  first argument. `dispatch_item_task` uses this to guarantee `finalize_task` always runs after an
  ingest task completes, without anything having to wait around for a result.

Celery has a lot more surface than this (task retries/rate-limits, richer canvas primitives like
groups and chords, a `Flower` monitoring UI, alternative brokers, etc.) - none of it is used here;
this project's own `src/observability_api.py` covers the "what's the state of my documents" need
instead.

## TODO

- Dedupe queued documents by content hash instead of file path, so a corrected file dropped
  back into `queue/` under the same name is picked up as new work instead of being silently
  ignored (see `queue_db.enqueue` / `tasks.poll_and_enqueue_task`).

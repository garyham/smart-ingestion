# smart-files

A proof-of-concept document ingestion pipeline: drop source documents into `queue/`, a Prefect
deployment polls on a cron schedule and ingests new arrivals into `ingested/` (metadata, chunks,
and for `.xlsx` a queryable DuckDB database).

## Installation

Requires Python >=3.12 and [`uv`](https://docs.astral.sh/uv/).

```bash
# clone the repo, then from its root:
uv sync
```

This installs the pinned dependencies from `uv.lock` (Prefect, pymupdf4llm, markitdown, duckdb,
langchain_text_splitters, etc.) into a local `.venv`.

## Operation

Ingestion needs a running Prefect server (it backs the poller's cron schedule) and the poller
itself, in two separate terminals:

```bash
# terminal 1: start the Prefect server
./prefect_server start

# terminal 2: start the poller - watches queue/ and ingests new arrivals into ingested/
uv run poll
```

With both running, drop a document into `queue/` and it's picked up on the next poll (every
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
# stop the poller: Ctrl-C in its terminal

# stop the Prefect server
./prefect_server stop

# wipe all flow run history from the Prefect server (useful when its UI/DB gets cluttered
# during development)
uv run clean-runs
```

`queue/` and `state/` (the poller's SQLite queue tracking what's been seen/processed) are
gitignored, local-only working directories.

## TODO

- Dedupe queued documents by content hash instead of file path, so a corrected file dropped
  back into `queue/` under the same name is picked up as new work instead of being silently
  ignored (see `queue_db.enqueue_new` / `poll_ingest.scan_queue_dir`).

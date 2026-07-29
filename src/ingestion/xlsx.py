"""xlsx ingestion: wraps a workbook in DuckDB and extracts metadata via standard queries.

Pure DuckDB library with no orchestration-framework dependency; the `run_xlsx_ingest` task
that calls into this module lives in `ingestion/xlsx_ingest.py`.
"""

import re
from pathlib import Path

import duckdb

METADATA_SHEETS = {"Contents", "Background Information", "Notes"}


def slugify(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = text.strip("_") or "col"
    if text[0].isdigit():
        # Leading digits (e.g. a "2024" year column) aren't valid in an unquoted SQL
        # identifier: DuckDB lexes "2024_in_service" as the number 2024 followed by a
        # stray token, breaking any expression built around it.
        text = f"_{text}"
    return text


def get_layers(con: duckdb.DuckDBPyConnection, path: str) -> list[str]:
    rows = con.execute("SELECT unnest(layers).name FROM st_read_meta(?)", [path]).fetchall()
    return [r[0] for r in rows]


def parse_contents_titles(con: duckdb.DuckDBPyConnection, path: str) -> dict[str, str]:
    """Map sheet number (as string) -> title, parsed from the Contents sheet."""
    titles: dict[str, str] = {}
    try:
        rows = con.execute("SELECT Field1 FROM st_read(?, layer='Contents')", [path]).fetchall()
    except duckdb.Error:
        return titles
    pattern = re.compile(r"Worksheet\s+(\d+)\s*[–-]\s*(.+)")
    for (text,) in rows:
        if not text:
            continue
        m = pattern.match(text.strip())
        if m:
            titles[m.group(1)] = m.group(2).strip()
    return titles


def read_sheet_text(con: duckdb.DuckDBPyConnection, path: str, layer: str) -> str:
    rows = con.execute("SELECT Field1 FROM st_read(?, layer=?)", [path, layer]).fetchall()
    return "\n".join(r[0] for r in rows if r[0])


def try_parse_number(value):
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return None


def infer_column_types(rows: list[tuple], n_cols: int) -> list[str]:
    types = []
    for c in range(n_cols):
        values = [row[c] for row in rows if row[c] is not None]
        if not values:
            types.append("VARCHAR")
        elif all(re.fullmatch(r"-?\d+", v.strip()) for v in values):
            types.append("BIGINT")
        elif all(try_parse_number(v) is not None for v in values):
            types.append("DOUBLE")
        else:
            types.append("VARCHAR")
    return types


def ingest_sheet(
    con: duckdb.DuckDBPyConnection, path: str, layer: str, table_name: str, title: str
) -> bool:
    """Detect the header row and load one numbered data sheet into a clean table."""
    cur = con.execute("SELECT * FROM st_read(?, layer=?)", [path, layer])
    rows = [row[1:] for row in cur.fetchall()]  # drop OGC_FID

    header_idx = next(
        (i for i, row in enumerate(rows) if sum(v is not None for v in row) >= 2), None
    )
    if header_idx is None:
        return False

    header_row = rows[header_idx]
    n_cols = 0
    for v in header_row:
        if v is None:
            break
        n_cols += 1
    if n_cols < 2:
        return False

    seen: dict[str, int] = {}
    columns = []
    for h in header_row[:n_cols]:
        base = slugify(h) if h else "col"
        count = seen.get(base, 0)
        seen[base] = count + 1
        columns.append(base if count == 0 else f"{base}_{count}")

    data_rows = []
    for row in rows[header_idx + 1 :]:
        trimmed = row[:n_cols]
        if all(v is None for v in trimmed):
            break
        data_rows.append(trimmed)
    if not data_rows:
        return False

    col_types = infer_column_types(data_rows, n_cols)
    col_defs = ", ".join(f'"{c}" {t}' for c, t in zip(columns, col_types))
    con.execute(f'CREATE TABLE "{table_name}" ({col_defs})')

    cast_rows = [
        [
            try_parse_number(v) if v is not None and t in ("BIGINT", "DOUBLE") else v
            for v, t in zip(row, col_types)
        ]
        for row in data_rows
    ]
    placeholders = ", ".join(["?"] * n_cols)
    con.executemany(f'INSERT INTO "{table_name}" VALUES ({placeholders})', cast_rows)

    safe_title = title.replace("'", "''")
    con.execute(f'COMMENT ON TABLE "{table_name}" IS \'{safe_title}\'')
    for c, h in zip(columns, header_row[:n_cols]):
        if h:
            safe_h = h.replace("'", "''")
            con.execute(f'COMMENT ON COLUMN "{table_name}"."{c}" IS \'{safe_h}\'')
    return True


def load_workbook(path: Path, db_path: str | Path = ":memory:") -> tuple[duckdb.DuckDBPyConnection, dict]:
    """Wrap the workbook in a DuckDB connection, one table per numbered sheet.

    `db_path` defaults to an ephemeral in-memory database; pass a file path to
    persist the ingested tables as a queryable .duckdb node instead.
    """
    con = duckdb.connect(str(db_path))
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")

    path_str = str(path)
    layers = get_layers(con, path_str)
    titles = parse_contents_titles(con, path_str)

    background = []
    table_titles: dict[str, str] = {}
    failed_sheets: list[str] = []
    for layer in layers:
        if layer in METADATA_SHEETS:
            text = read_sheet_text(con, path_str, layer)
            if text:
                background.append(f"## {layer}\n{text}")
            continue
        if not re.fullmatch(r"\d+", layer):
            continue
        title = titles.get(layer, f"Sheet {layer}")
        table_name = f"sheet_{layer}"
        if ingest_sheet(con, path_str, layer, table_name, title):
            table_titles[table_name] = title
        else:
            failed_sheets.append(layer)

    return con, {
        "background": "\n\n".join(background),
        "tables": table_titles,
        "failed_sheets": failed_sheets,
    }


def compute_column_stats(
    con: duckdb.DuckDBPyConnection, table_name: str, column: str, dtype: str, original_name: str | None
) -> dict:
    """Run a fixed set of standard summary queries for one column.

    `original_name` is the header text as it appeared in the spreadsheet, before
    slugifying to a SQL-safe column name (e.g. "Revenue (£m)" -> "revenue_m") — kept
    so a unit or phrasing hint isn't lost when an LLM is deciding how to query the
    column.

    Future: an LLM could look at each sheet's title/background and infer
    additional, sheet-specific queries beyond this fixed set (e.g. group-by
    breakdowns for a particular category column). Not needed yet.
    """
    if dtype in ("BIGINT", "DOUBLE"):
        null_count, min_v, max_v, avg_v = con.execute(
            f'SELECT count(*) - count("{column}"), min("{column}"), max("{column}"), avg("{column}") '
            f'FROM "{table_name}"'
        ).fetchone()
        return {
            "name": column,
            "original_name": original_name,
            "type": dtype,
            "null_count": null_count,
            "min": min_v,
            "max": max_v,
            "avg": avg_v,
        }

    null_count, distinct_count = con.execute(
        f'SELECT count(*) - count("{column}"), count(DISTINCT "{column}") FROM "{table_name}"'
    ).fetchone()
    top_values = con.execute(
        f'SELECT "{column}", count(*) AS c FROM "{table_name}" '
        f'WHERE "{column}" IS NOT NULL GROUP BY 1 ORDER BY c DESC LIMIT 5'
    ).fetchall()
    return {
        "name": column,
        "original_name": original_name,
        "type": dtype,
        "null_count": null_count,
        "distinct_count": distinct_count,
        "top_values": [{"value": v, "count": c} for v, c in top_values],
    }


def compute_table_metadata(con: duckdb.DuckDBPyConnection, table_name: str, title: str) -> dict:
    row_count = con.execute(f'SELECT count(*) FROM "{table_name}"').fetchone()[0]
    columns = con.execute(f'DESCRIBE "{table_name}"').fetchall()  # (name, type, null, key, default, extra)
    comments = dict(
        con.execute(
            "SELECT column_name, comment FROM duckdb_columns() WHERE table_name = ?", [table_name]
        ).fetchall()
    )
    return {
        "table_name": table_name,
        "title": title,
        "row_count": row_count,
        "columns": [
            compute_column_stats(con, table_name, name, dtype, comments.get(name))
            for name, dtype, *_ in columns
        ],
    }


def extract_metadata(path: Path, db_path: str | Path = ":memory:") -> dict:
    """Load `path` into DuckDB and run the standard queries, returning a JSON-able dict.

    If `db_path` is a file path, the ingested tables are left persisted there as a
    queryable DuckDB database once this returns.
    """
    con, meta = load_workbook(path, db_path)
    try:
        tables = [compute_table_metadata(con, name, title) for name, title in meta["tables"].items()]
    finally:
        con.close()

    return {
        "background": meta["background"],
        "tables": tables,
        "failed_sheets": meta["failed_sheets"],
    }

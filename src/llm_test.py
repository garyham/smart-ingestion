"""Standalone test harness for the LLM/query side of xlsx ingestion.

Not part of the ingestion pipeline (see CLAUDE.md: downstream consumption is out of
scope there). This simulates what happens *after* a vector search has already
surfaced an ingested xlsx document: given its `metadata`, build a schema-aware
system prompt and a `query_dataset` tool bound to that document's `.duckdb` file,
then let the model answer a question via litellm tool-calling.

Usage (from the repo root, so relative `ingested/` paths resolve):
    uv run ask "How many vessels were in service in 2023?"
    uv run ask "..." --doc UK_armed_forces_equipment_and_formations_2025
    uv run ask "..." --model anthropic/claude-sonnet-5  # default is gpt-4.1
"""

import argparse
import json
import os
import re
from pathlib import Path

import duckdb
import litellm

DEFAULT_DOC = "UK_armed_forces_equipment_and_formations_2025"
DEFAULT_MODEL = os.environ.get("LLM_TEST_MODEL", "gpt-4.1")
MAX_ROWS = 200
MAX_TOOL_ROUNDS = 6

FORBIDDEN_SQL = re.compile(
    r"\b(insert|update|delete|drop|alter|create|attach|detach|copy|pragma|call|export|import)\b",
    re.IGNORECASE,
)

QUERY_TOOL_SCHEMA = {
    "type": "function",
    "function": {
        "name": "query_dataset",
        "description": (
            "Run a read-only SQL SELECT query against this dataset's DuckDB database "
            "and return the resulting rows. Use exact table/column names as given in "
            "the schema description."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "A DuckDB SQL SELECT statement."},
            },
            "required": ["sql"],
        },
    },
}


def load_metadata(doc_dir: Path) -> dict:
    return json.loads((doc_dir / "metadata").read_text())


def format_schema_prompt(metadata: dict) -> str:
    lines = [f"Dataset: {metadata['title']}"]
    if metadata.get("background"):
        lines.append(metadata["background"])

    for table in metadata["tables"]:
        lines.append(f'\nTable "{table["table_name"]}" ({table["title"]}), {table["row_count"]} rows:')
        for col in table["columns"]:
            desc = f"  - {col['name']} ({col['type']})"
            if col.get("original_name") and col["original_name"] != col["name"]:
                original = col["original_name"].replace("\n", " ")
                desc += f' [originally "{original}"]'
            if "top_values" in col:
                sample = ", ".join(str(v["value"]) for v in col["top_values"][:3])
                if sample:
                    desc += f" e.g. {sample}"
            elif col.get("min") is not None:
                desc += f" range {col['min']}-{col['max']}"
            lines.append(desc)

    return "\n".join(lines)


def make_query_tool(db_path: Path):
    def query_dataset(sql: str) -> str:
        if FORBIDDEN_SQL.search(sql):
            return json.dumps({"error": "Only read-only SELECT queries are allowed."})
        con = duckdb.connect(str(db_path), read_only=True)
        try:
            cur = con.execute(sql)
            columns = [d[0] for d in cur.description]
            rows = cur.fetchmany(MAX_ROWS)
        except duckdb.Error as e:
            return json.dumps({"error": str(e)})
        finally:
            con.close()
        return json.dumps({"columns": columns, "rows": rows}, default=str)

    return query_dataset


def run(question: str, doc_dir: Path, model: str) -> str:
    metadata = load_metadata(doc_dir)
    db_path = doc_dir / metadata["duckdb_file"]
    query_dataset = make_query_tool(db_path)

    system_prompt = (
        "You answer questions about a single dataset by calling the query_dataset "
        "tool with a DuckDB SQL SELECT statement. Always query for facts rather than "
        "guessing at values.\n\n" + format_schema_prompt(metadata)
    )
    messages: list[dict] = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": question},
    ]

    for _ in range(MAX_TOOL_ROUNDS):
        response = litellm.completion(model=model, messages=messages, tools=[QUERY_TOOL_SCHEMA])
        message = response.choices[0].message
        messages.append(message.model_dump())

        if not message.tool_calls:
            return message.content

        for tool_call in message.tool_calls:
            args = json.loads(tool_call.function.arguments)
            print(f"  [tool call] {args['sql']}")
            result = query_dataset(args["sql"])
            messages.append(
                {"role": "tool", "tool_call_id": tool_call.id, "content": result}
            )

    return "(gave up after too many tool-call rounds)"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("question")
    parser.add_argument("--doc", default=DEFAULT_DOC, help="ingested/<doc> folder name")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="litellm model string")
    args = parser.parse_args()

    doc_dir = Path("ingested") / args.doc
    answer = run(args.question, doc_dir, args.model)
    print(answer)


if __name__ == "__main__":
    main()

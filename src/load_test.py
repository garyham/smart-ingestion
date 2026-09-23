import argparse
import mimetypes
import os
import time
from pathlib import Path
from uuid import UUID

import httpx
from prefect.client.orchestration import get_client

API_URL = os.getenv("SMART_FILES_API_URL", "http://127.0.0.1:8000").rstrip("/")


def positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def upload_details(path: Path) -> dict[str, str | int]:
    content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    return {
        "filename": path.name,
        "content_type": content_type,
        "size": path.stat().st_size,
    }


def wait_for_completions(expected: set[UUID], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    completed: set[UUID] = set()
    reported = -1
    while completed != expected:
        with get_client(sync_client=True) as client:
            for task_run_id in expected - completed:
                task_run = client.read_task_run(task_run_id)
                if task_run.state and task_run.state.is_failed():
                    raise RuntimeError(
                        f"ingestion task {task_run_id} failed: {task_run.state.message}"
                    )
                if task_run.state and task_run.state.is_final():
                    completed.add(task_run_id)
        if len(completed) != reported:
            reported = len(completed)
            print(f"Completed {reported}/{len(expected)}")
        if time.monotonic() >= deadline:
            missing = len(expected - completed)
            raise TimeoutError(f"timed out with {missing} ingestion(s) unfinished")
        time.sleep(1)


def run(count: int, data_dir: Path, timeout: float) -> None:
    """Drive the real upload path: presign, write bytes, notify, wait.

    Each iteration is a new logical document, but identical bytes are deduplicated by
    content, so reuploading the same file exercises the reuse path rather than the
    operators. Point `--data-dir` at least `--count` distinct files to load the operators.
    """
    files = sorted(path for path in data_dir.iterdir() if path.is_file())
    if not files:
        raise SystemExit(f"No files found in {data_dir}")

    task_run_ids: set[UUID] = set()
    for index in range(count):
        path = files[index % len(files)]
        details = upload_details(path)

        presign = httpx.post(f"{API_URL}/presign", json=details, timeout=30)
        presign.raise_for_status()
        session = presign.json()

        upload = httpx.put(
            session["upload_url"],
            content=path.read_bytes(),
            headers=session["upload_headers"],
            timeout=120,
        )
        upload.raise_for_status()

        notify = httpx.post(
            f"{API_URL}/notify",
            json={"document_id": session["document_id"]},
            timeout=60,
        )
        notify.raise_for_status()
        task_run_ids.add(UUID(notify.json()["task_run_id"]))

    print(f"Queued {count} notification(s)")
    wait_for_completions(task_run_ids, timeout)


def main() -> None:
    parser = argparse.ArgumentParser(description="Stress test file ingestion")
    parser.add_argument("--count", type=positive_int, required=True)
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--timeout", type=positive_int, default=1800, help="seconds")
    args = parser.parse_args()
    run(args.count, args.data_dir, args.timeout)


if __name__ == "__main__":
    main()

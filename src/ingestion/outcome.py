import json
from pathlib import Path


def read_outcome(output_root: Path, doc: Path) -> tuple[bool, str | None]:
    """Read back status.json for a document; returns (succeeded, error_detail)."""
    status_path = output_root / doc.stem / "status.json"
    if not status_path.exists():
        return False, "no status.json was written"
    status = json.loads(status_path.read_text())
    if status.get("status") == "ok":
        return True, None
    reason = status.get("reason", status.get("status"))
    detail = status.get("detail")
    return False, f"{reason}: {detail}" if detail else str(reason)

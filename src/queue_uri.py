from pathlib import Path
from urllib.parse import unquote, urlparse


def to_file_uri(path: Path) -> str:
    return path.resolve().as_uri()


def from_file_uri(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise ValueError(f"unsupported queue URI scheme: {uri!r} (only file:// is supported so far)")
    return Path(unquote(parsed.path))

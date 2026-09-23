from ingestion.config import load_config
from ingestion.routing import ensure_concurrency_limits
from store.schema import ensure_schema


def initialize() -> None:
    """Prepare Smart Files before any runtime service starts."""
    ensure_schema()
    config = load_config()
    ensure_concurrency_limits(config.get("concurrency_limits", {}))


def main() -> None:
    initialize()

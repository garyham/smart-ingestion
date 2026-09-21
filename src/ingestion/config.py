from pathlib import Path

import yaml


def load_config(config_path: Path = Path("config/config.yaml")) -> dict:
    """Load the ingestion concurrency configuration."""
    return yaml.safe_load(config_path.read_text())

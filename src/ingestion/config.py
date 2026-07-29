from pathlib import Path

import yaml


def load_config(config_path: Path = Path("config/config.yaml")) -> dict:
    """Load the pipeline's general configuration (MIME whitelist, doc pool size, ...)."""
    return yaml.safe_load(config_path.read_text())

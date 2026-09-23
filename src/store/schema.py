"""The store's schema: one Alembic chain for every table in `smart_files`."""

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import text

from db import engine


def ensure_schema() -> None:
    """Run Alembic migrations while holding the shared schema lock."""
    config = Config()
    config.set_main_option(
        "script_location", str(Path(__file__).with_name("migrations"))
    )
    with engine.begin() as connection:
        connection.execute(
            text("SELECT pg_advisory_xact_lock(hashtext('smart_files_schema'))")
        )
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS smart_files"))
        config.attributes["connection"] = connection
        command.upgrade(config, "head")

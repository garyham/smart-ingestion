"""Shared engine, session factory, and declarative base for every storage module.

Each service owns its own tables, but they all live in the `smart_files` schema and share
one connection pool and one `Base`, so Alembic sees a single metadata and a process opens
a single pool no matter how many services it hosts.
"""

import os

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

SCHEMA = "smart_files"
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://smart_files:smart_files@127.0.0.1:5435/smart_files",
)


def sqlalchemy_url(url: str) -> str:
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


class Base(DeclarativeBase):
    pass


engine = create_engine(sqlalchemy_url(DATABASE_URL), pool_pre_ping=True)
SessionLocal = sessionmaker(engine, expire_on_commit=False)

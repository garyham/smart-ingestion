from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import CheckConstraint, DateTime, Index, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

INGESTION_SCHEMA = "smart_files"


class Base(DeclarativeBase):
    pass


class Ingestion(Base):
    __tablename__ = "ingestions"
    __table_args__ = (
        CheckConstraint(
            "source_sha256 ~ '^[0-9a-f]{64}$'", name="ingestions_sha256_check"
        ),
        CheckConstraint(
            "status IN ('processing', 'published', 'embedding', 'completed', 'failed')",
            name="ingestions_status_check",
        ),
        UniqueConstraint(
            "source_sha256",
            "pipeline_version",
            name="ingestions_source_pipeline_key",
        ),
        Index("ingestions_status_idx", "status", "updated_at"),
        {"schema": INGESTION_SCHEMA},
    )

    ingestion_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True), primary_key=True
    )
    document_id: Mapped[UUID] = mapped_column(
        PostgreSQLUUID(as_uuid=True), nullable=False
    )
    source_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    pipeline_version: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    current_step: Mapped[str] = mapped_column(String, nullable=False)
    source: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    steps: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default="{}"
    )
    outputs: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default="{}"
    )
    error: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

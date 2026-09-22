import hashlib
import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

from ingestion.models import Ingestion
from ingestion.schemas import (
    IngestionCreate,
    IngestionRead,
    IngestionStatus,
    IngestionUpdate,
)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://smart_files:smart_files@127.0.0.1:5435/smart_files",
)
PIPELINE_VERSION = os.getenv("PIPELINE_VERSION", "1")


def _sqlalchemy_url(url: str) -> str:
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


engine = create_engine(_sqlalchemy_url(DATABASE_URL), pool_pre_ping=True)
SessionLocal = sessionmaker(engine, expire_on_commit=False)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


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


def _record(record: Ingestion) -> dict:
    return IngestionRead.model_validate(record).model_dump(mode="python")


def claim_ingestion(
    ingestion_id: str,
    document_id: str,
    source_sha256: str,
    source: dict,
) -> tuple[bool, dict]:
    """Claim content for this pipeline version, or return its canonical ingestion."""
    now = datetime.now(UTC)
    request = IngestionCreate(
        ingestion_id=ingestion_id,
        document_id=document_id,
        source_sha256=source_sha256,
        pipeline_version=PIPELINE_VERSION,
        source=source,
    )
    values = {
        **request.model_dump(mode="python"),
        "status": IngestionStatus.PROCESSING.value,
        "current_step": "routing",
        "source": source,
        "steps": {"routing": {"status": "processing", "updated_at": now.isoformat()}},
        "outputs": {},
        "created_at": now,
        "updated_at": now,
    }
    with SessionLocal.begin() as session:
        statement = (
            insert(Ingestion)
            .values(**values)
            .on_conflict_do_nothing(
                index_elements=["source_sha256", "pipeline_version"]
            )
            .returning(Ingestion)
        )
        record = session.scalars(statement).one_or_none()
        if record is not None:
            return True, _record(record)

        record = session.scalars(
            select(Ingestion)
            .where(
                Ingestion.source_sha256 == source_sha256,
                Ingestion.pipeline_version == PIPELINE_VERSION,
            )
            .with_for_update()
        ).one()
        owned = str(record.ingestion_id) == ingestion_id
        if owned and record.status == IngestionStatus.FAILED.value:
            record.status = IngestionStatus.PROCESSING.value
            record.current_step = "routing"
            record.error = None
            record.steps = {
                **record.steps,
                "routing": {"status": "processing", "updated_at": now.isoformat()},
            }
            record.updated_at = now
            record.completed_at = None
        return owned, _record(record)


def update_ingestion(
    ingestion_id: str,
    status: str,
    current_step: str,
    *,
    outputs: dict | None = None,
    error: dict | None = None,
) -> None:
    now = datetime.now(UTC)
    change = IngestionUpdate(
        status=status,
        current_step=current_step,
        outputs=outputs or {},
        error=error,
    )
    with SessionLocal.begin() as session:
        record = session.scalars(
            select(Ingestion)
            .where(Ingestion.ingestion_id == UUID(ingestion_id))
            .with_for_update()
        ).one()
        record.status = change.status.value
        record.current_step = change.current_step
        record.outputs = {**record.outputs, **change.outputs}
        record.steps = {
            **record.steps,
            change.current_step: {
                "status": change.status.value,
                "updated_at": now.isoformat(),
            },
        }
        record.error = change.error
        record.updated_at = now
        if change.status == IngestionStatus.COMPLETED:
            record.completed_at = now


def get_ingestion(ingestion_id: UUID) -> IngestionRead | None:
    with SessionLocal() as session:
        record = session.get(Ingestion, ingestion_id)
        return IngestionRead.model_validate(record) if record else None


def list_ingestions(
    *,
    status: IngestionStatus | None = None,
    source_sha256: str | None = None,
    document_id: UUID | None = None,
    pipeline_version: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[IngestionRead]:
    statement = select(Ingestion)
    if status is not None:
        statement = statement.where(Ingestion.status == status.value)
    if source_sha256 is not None:
        statement = statement.where(Ingestion.source_sha256 == source_sha256)
    if document_id is not None:
        statement = statement.where(Ingestion.document_id == document_id)
    if pipeline_version is not None:
        statement = statement.where(Ingestion.pipeline_version == pipeline_version)
    statement = (
        statement.order_by(Ingestion.created_at.desc()).limit(limit).offset(offset)
    )
    with SessionLocal() as session:
        return [IngestionRead.model_validate(row) for row in session.scalars(statement)]

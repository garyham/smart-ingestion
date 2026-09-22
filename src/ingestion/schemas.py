from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class IngestionStatus(StrEnum):
    PROCESSING = "processing"
    PUBLISHED = "published"
    EMBEDDING = "embedding"
    COMPLETED = "completed"
    FAILED = "failed"


class IngestionCreate(BaseModel):
    ingestion_id: UUID
    document_id: UUID
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    pipeline_version: str = Field(min_length=1)
    source: dict[str, Any]


class IngestionUpdate(BaseModel):
    status: IngestionStatus
    current_step: str = Field(min_length=1)
    outputs: dict[str, Any] = Field(default_factory=dict)
    error: dict[str, Any] | None = None


class IngestionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    ingestion_id: UUID
    document_id: UUID
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    pipeline_version: str
    status: IngestionStatus
    current_step: str
    source: dict[str, Any]
    steps: dict[str, Any]
    outputs: dict[str, Any]
    error: dict[str, Any] | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None

"""Pipeline releases: an immutable composition of operator versions.

A release selects operator versions and nothing more. It is not part of any reuse key, so
publishing a new release does not invalidate work an earlier release already produced, and
changing the active release never starts a backfill.
"""

from functools import cache

from pydantic import BaseModel, Field

from contracts.operators import OperatorRef
from ingestion.config import load_config


class Release(BaseModel):
    name: str = Field(min_length=1)
    extractor: str
    chunker: str
    embedders: list[str] = Field(default_factory=list)
    recognisers: list[str] = Field(default_factory=list)

    @property
    def extractor_ref(self) -> OperatorRef:
        return OperatorRef.parse(self.extractor)

    @property
    def chunker_ref(self) -> OperatorRef:
        return OperatorRef.parse(self.chunker)

    @property
    def embedder_refs(self) -> list[OperatorRef]:
        return [OperatorRef.parse(value) for value in self.embedders]

    @property
    def recogniser_refs(self) -> list[OperatorRef]:
        return [OperatorRef.parse(value) for value in self.recognisers]


def parse_release(config: dict) -> Release:
    release = config.get("release")
    if not release:
        raise ValueError("config/config.yaml has no active `release` block")
    return Release.model_validate(release)


@cache
def active_release() -> Release:
    """The release new documents are ingested with, and searches default to."""
    return parse_release(load_config())

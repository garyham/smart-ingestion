"""Operator identity: a `name@version` reference.

An operator version is immutable - its configuration is fixed by the version, so changing
what an operator produces means publishing a new version. A result is reusable when the
operator version and the input identities match. `pipeline_version` is deliberately not
part of that key - a release only selects operator versions, it does not change what they
produce.
"""

from typing import Self

from pydantic import BaseModel, ConfigDict, Field


class OperatorRef(BaseModel):
    """One immutable operator version, written `chunker@1`."""

    model_config = ConfigDict(frozen=True)

    name: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9_-]*$")
    version: str = Field(min_length=1, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

    @classmethod
    def parse(cls, value: "str | OperatorRef") -> Self:
        if isinstance(value, OperatorRef):
            return cls(name=value.name, version=value.version)
        name, separator, version = str(value).partition("@")
        if not separator:
            raise ValueError(f"operator must be written name@version, not {value!r}")
        return cls(name=name, version=version)

    def __str__(self) -> str:
        return f"{self.name}@{self.version}"

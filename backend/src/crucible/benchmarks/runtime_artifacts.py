"""Host-owned dependency inventory, obtained only from the pinned task image."""

from pathlib import PurePosixPath
from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from crucible.benchmarks.contracts import Sha256


class RuntimeArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    size_bytes: Annotated[int, Field(ge=0, le=64 * 1024 * 1024)]
    sha256: Sha256


class RuntimeArtifactInventory(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    image: Annotated[str, Field(pattern=r"^[^\s@]+@sha256:[0-9a-f]{64}$")]
    files: Annotated[dict[str, RuntimeArtifact], Field(max_length=4096)]

    @model_validator(mode="after")
    def bounded_safe_paths(self) -> Self:
        for name in self.files:
            path = PurePosixPath(name)
            if (
                not name
                or not path.parts
                or path.is_absolute()
                or path.as_posix() != name
                or any(p.lower() in ("..", ".git") for p in path.parts)
                or any(c in name for c in "\0\\\r\n")
            ):
                raise ValueError("unsafe runtime artifact path")
        if sum(f.size_bytes for f in self.files.values()) > 512 * 1024 * 1024:
            raise ValueError("runtime inventory exceeds dependency budget")
        return self

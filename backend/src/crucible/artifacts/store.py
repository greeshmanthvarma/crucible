import asyncio
import hashlib
import os
import tempfile
from collections.abc import AsyncIterable
from pathlib import Path, PurePosixPath

from crucible.domain.artifacts import Artifact
from crucible.domain.clock import Clock
from crucible.domain.ids import TaskId, new_id


class LocalArtifactStore:
    def __init__(self, root: Path, clock: Clock) -> None:
        self._root = root
        self._clock = clock

    async def put(
        self,
        task_id: TaskId,
        media_type: str,
        sensitivity: str,
        stream: AsyncIterable[bytes],
        *,
        hard_limit: int | None = None,
    ) -> Artifact:
        if hard_limit is not None and hard_limit < 0:
            raise ValueError("Artifact hard limit cannot be negative")
        await asyncio.to_thread(self._prepare_directories)
        descriptor, temporary_name = tempfile.mkstemp(dir=self._root / "tmp")
        temporary = Path(temporary_name)
        digest = hashlib.sha256()
        original_bytes = 0
        retained_bytes = 0
        try:
            with os.fdopen(descriptor, "wb") as output:
                os.chmod(temporary, 0o600)
                async for chunk in stream:
                    if not isinstance(chunk, bytes):
                        raise TypeError("Artifact chunks must be bytes")
                    original_bytes += len(chunk)
                    remaining = (
                        len(chunk)
                        if hard_limit is None
                        else max(0, hard_limit - retained_bytes)
                    )
                    retained = chunk[:remaining]
                    if retained:
                        output.write(retained)
                        digest.update(retained)
                        retained_bytes += len(retained)
                output.flush()
                os.fsync(output.fileno())

            content_hash = digest.hexdigest()
            identity = PurePosixPath("sha256", content_hash[:2], content_hash)
            destination = self._root.joinpath(*identity.parts)
            await asyncio.to_thread(destination.parent.mkdir, 0o700, True, True)
            await asyncio.to_thread(os.replace, temporary, destination)
            await asyncio.to_thread(os.chmod, destination, 0o600)
            return Artifact(
                new_id(),
                task_id,
                content_hash,
                media_type,
                retained_bytes,
                identity.as_posix(),
                sensitivity,
                {
                    "original_bytes": original_bytes,
                    "retained_bytes": retained_bytes,
                    "truncated": original_bytes > retained_bytes,
                },
                self._clock.now(),
            )
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

    async def read(self, storage_identity: str) -> bytes:
        path = self._path_for(storage_identity)
        return await asyncio.to_thread(path.read_bytes)

    def _prepare_directories(self) -> None:
        self._root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self._root, 0o700)
        temporary = self._root / "tmp"
        temporary.mkdir(mode=0o700, exist_ok=True)
        os.chmod(temporary, 0o700)

    def _path_for(self, storage_identity: str) -> Path:
        identity = PurePosixPath(storage_identity)
        if identity.is_absolute() or ".." in identity.parts:
            raise ValueError("Invalid Artifact storage identity")
        path = self._root.joinpath(*identity.parts)
        if not path.is_relative_to(self._root):
            raise ValueError("Artifact storage identity escapes its root")
        return path

import base64
import json
from dataclasses import dataclass

from crucible.sandbox.protocol import OutputChunk, OutputStream


@dataclass(frozen=True)
class RetentionMetadata:
    original_bytes: int
    retained_bytes: int
    truncated: bool


@dataclass(frozen=True)
class OutputMetadata:
    original_bytes: int
    stdout_bytes: int
    stderr_bytes: int
    live: RetentionMetadata
    model: RetentionMetadata
    artifact: RetentionMetadata


class OutputCapture:
    def __init__(
        self, *, live_limit: int, model_limit: int, artifact_limit: int
    ) -> None:
        if min(live_limit, model_limit, artifact_limit) < 0:
            raise ValueError("Output limits cannot be negative")
        self._limits = (live_limit, model_limit, artifact_limit)
        self._live = bytearray()
        self._model = bytearray()
        self._artifact = bytearray()
        self._artifact_chunks: list[OutputChunk] = []
        self._original_bytes = 0
        self._stdout_bytes = 0
        self._stderr_bytes = 0
        self._next_sequence = 1

    async def accept(self, chunk: OutputChunk) -> None:
        if chunk.sequence != self._next_sequence:
            raise ValueError(
                f"Expected output sequence {self._next_sequence}, got {chunk.sequence}"
            )
        self._next_sequence += 1
        self._original_bytes += len(chunk.data)
        if chunk.stream is OutputStream.STDOUT:
            self._stdout_bytes += len(chunk.data)
        else:
            self._stderr_bytes += len(chunk.data)
        self._retain(self._live, chunk.data, self._limits[0])
        self._retain(self._model, chunk.data, self._limits[1])
        remaining = max(0, self._limits[2] - len(self._artifact))
        retained = chunk.data[:remaining]
        if retained:
            self._artifact.extend(retained)
            self._artifact_chunks.append(
                OutputChunk(chunk.stream, chunk.sequence, retained)
            )

    @property
    def live(self) -> bytes:
        return bytes(self._live)

    @property
    def model(self) -> bytes:
        return bytes(self._model)

    @property
    def artifact(self) -> bytes:
        return bytes(self._artifact)

    @property
    def metadata(self) -> OutputMetadata:
        return OutputMetadata(
            self._original_bytes,
            self._stdout_bytes,
            self._stderr_bytes,
            self._retention(len(self._live)),
            self._retention(len(self._model)),
            self._retention(len(self._artifact)),
        )

    def artifact_records(self) -> list[dict[str, object]]:
        return [
            {
                "sequence": chunk.sequence,
                "stream": chunk.stream.value,
                "data_base64": base64.b64encode(chunk.data).decode("ascii"),
            }
            for chunk in self._artifact_chunks
        ]

    def artifact_ndjson(self) -> bytes:
        return b"".join(
            json.dumps(record, sort_keys=True, separators=(",", ":")).encode() + b"\n"
            for record in self.artifact_records()
        )

    @staticmethod
    def _retain(target: bytearray, data: bytes, limit: int) -> None:
        target.extend(data[: max(0, limit - len(target))])

    def _retention(self, retained: int) -> RetentionMetadata:
        return RetentionMetadata(
            self._original_bytes,
            retained,
            retained < self._original_bytes,
        )

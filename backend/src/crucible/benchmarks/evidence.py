"""Exclusive, append-only stage evidence and atomic terminal records."""

import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from crucible.repair.identity import canonical_json


def sanitize(value: object) -> object:
    if isinstance(value, str):
        for name, secret in os.environ.items():
            if secret and name.endswith(("API_KEY", "TOKEN", "SECRET", "PASSWORD")):
                value = value.replace(secret, "[REDACTED]")
        return value
    if isinstance(value, dict):
        return {str(key): sanitize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize(item) for item in value]
    return value


def atomic_json(path: Path, value: object) -> None:
    content = canonical_json(sanitize(json.loads(canonical_json(value))))
    descriptor, temporary = tempfile.mkstemp(prefix=".record-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(temporary).unlink(missing_ok=True)


def _safe_root(root: Path) -> None:
    if (
        not root.is_absolute()
        or ".." in root.parts
        or any(p.is_symlink() for p in [root, *root.parents])
    ):
        raise ValueError("invalid evidence root")


class TrialEvidence:
    def __init__(self, root: Path, request: object) -> None:
        _safe_root(root)
        root.mkdir(parents=True, mode=0o700, exist_ok=False)
        self.root = root
        self.current_stage = "prepared"
        self.operation = (
            request.get("operation") if isinstance(request, dict) else "trial"
        )
        atomic_json(root / "request.json", request)
        self._event("prepared")

    def _event(self, stage: str) -> None:
        event = {
            "schema_version": 1,
            "stage": stage,
            "at": datetime.now(UTC).isoformat(),
        }
        with (self.root / "events.jsonl").open("ab") as stream:
            stream.write(canonical_json(event) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())

    def stage(self, stage: str) -> None:
        transitions = {
            "prepared": "solving",
            "solving": "snapshotting",
            "snapshotting": "grading",
            "grading": "complete",
        }
        if (
            stage not in {"error", "interrupted"}
            and transitions.get(self.current_stage) != stage
            and not (
                self.operation == "admit"
                and self.current_stage == "prepared"
                and stage == "grading"
            )
        ):
            raise ValueError("invalid evidence stage transition")
        if self.current_stage in {"complete", "error", "interrupted"}:
            raise ValueError("terminal evidence cannot be resumed")
        self._event(stage)
        self.current_stage = stage

    def finish(self, result: object, status: str) -> None:
        if (self.root / "result.json").exists():
            raise ValueError("terminal result already exists")
        value = json.loads(canonical_json(result))
        self._artifact_refs(value)
        terminal = (
            "complete"
            if status == "complete"
            else "interrupted"
            if status == "interrupted"
            else "error"
        )
        if terminal == "complete" and self.current_stage != "grading":
            raise ValueError("cannot complete evidence before grading")
        # Result commits the terminal state; an event alone cannot establish completion.
        atomic_json(self.root / "result.json", value)
        self.stage(terminal)

    def _artifact_refs(self, value: object) -> None:
        if not isinstance(value, dict):
            return
        for key, item in value.items():
            if isinstance(item, dict):
                self._artifact_refs(item)
            elif (
                key in {"path", "trace_path", "usage_path", "report_path", "log_path"}
                and item is not None
            ):
                path = Path(item)
                resolved = (
                    path.resolve()
                    if path.is_absolute()
                    else (self.root / path).resolve()
                )
                if not resolved.is_relative_to(self.root.resolve()) or any(
                    p.is_symlink() for p in [resolved, *resolved.parents]
                ):
                    raise ValueError("artifact reference escapes controller output")
                value[key] = resolved.relative_to(self.root.resolve()).as_posix()

    @staticmethod
    def show(root: Path) -> dict[str, object]:
        _safe_root(root)
        for name in ("request.json", "events.jsonl", "result.json"):
            path = root / name
            if path.is_symlink() or (path.exists() and path.stat().st_size > 1_048_576):
                raise ValueError("invalid or oversized evidence")
        request = json.loads((root / "request.json").read_text())
        lines = (root / "events.jsonl").read_bytes().splitlines(keepends=True)
        events = []
        for index, line in enumerate(lines):
            try:
                events.append(json.loads(line))
            except (ValueError, UnicodeDecodeError):
                if index == len(lines) - 1 and not line.endswith(b"\n"):
                    break
                raise ValueError("invalid stage event") from None
        stage = events[-1]["stage"] if events else "prepared"
        result_path = root / "result.json"
        result = json.loads(result_path.read_text()) if result_path.exists() else None
        status = result.get("status", "error") if result else "interrupted"
        grade = result.get("grade") if result else None
        identity = request.get("identity", {})
        return cast(
            dict[str, object],
            sanitize(
                {
                    "schema_version": 1,
                    "trial_id": identity.get("trial_id", request.get("trial_id")),
                    "status": str(status)[:40],
                    "stage": str(stage)[:40],
                    "error_code": str(result.get("error_code"))[:200]
                    if result
                    else "incomplete_evidence",
                    "verdict": grade.get("verdict")
                    if isinstance(grade, dict)
                    else None,
                }
            ),
        )

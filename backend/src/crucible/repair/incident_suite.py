"""Private immutable accepted incidents, separate from candidate source trees."""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID

from crucible.repair.cases import ApprovedCase
from crucible.repair.identity import canonical_json, digest
from crucible.repair.trials import fixture_manifest, private_artifact


@dataclass(frozen=True)
class AcceptedIncident:
    case_digest: str
    definition_artifact_id: UUID
    fixture_manifest_artifact_id: UUID
    accepted_commit: str
    created_at: datetime


class IncidentSuite:
    def __init__(
        self,
        root: Path,
        unit_of_work: Callable[[], UnitOfWork],
        artifacts: Any,
        clock: Any,
        *,
        fixtures: Mapping[str, Path],
    ) -> None:
        self.root, self.factory = root, unit_of_work
        self.artifacts, self.clock, self.fixtures = artifacts, clock, fixtures

    def _directory(self) -> None:
        for component in (self.root, *self.root.parents):
            if component.is_symlink():
                raise ValueError("Private suite path cannot follow symlinks")
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root.chmod(0o700)

    async def _read(self, artifact_id: UUID) -> bytes:
        metadata, raw = await self.artifacts.read(artifact_id)
        if (
            metadata.sensitivity != "private"
            or hashlib.sha256(raw).hexdigest() != metadata.content_hash
            or len(raw) != metadata.byte_length
        ):
            raise ValueError("Private incident artifact integrity failure")
        return bytes(raw)

    async def register(
        self, case: ApprovedCase, accepted_commit: str
    ) -> AcceptedIncident:
        if not isinstance(case, ApprovedCase):
            raise TypeError("Only approved cases can be accepted")
        case = ApprovedCase.model_validate_json(case.model_dump_json())
        if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", accepted_commit):
            raise ValueError("Acceptance must identify a full commit")
        self._directory()
        for existing in await self.list_accepted():
            if existing.case_digest == case.case_digest:
                if existing.accepted_commit != accepted_commit:
                    raise ValueError("Incident acceptance is immutable")
                return existing
        fixture = self.fixtures.get(case.case.fixture_manifest_digest)
        if fixture is None:
            raise ValueError("Fixture is unavailable")
        manifest = fixture_manifest(fixture)
        if digest(manifest) != case.case.fixture_manifest_digest:
            raise ValueError("Fixture digest changed")
        task_id = case.case.incident_ids["task"]
        definition = await private_artifact(
            self.artifacts, task_id, case.model_dump_json().encode()
        )
        fixture_id = await private_artifact(
            self.artifacts, task_id, canonical_json(manifest)
        )
        record = AcceptedIncident(
            case.case_digest, definition, fixture_id, accepted_commit, self.clock.now()
        )
        # Exclusive reference creation prevents an accidental filesystem overwrite.
        reference = self.root / (case.case_digest + ".json")
        raw = canonical_json(
            {
                "case_digest": record.case_digest,
                "definition_artifact_id": record.definition_artifact_id,
                "fixture_manifest_artifact_id": record.fixture_manifest_artifact_id,
                "accepted_commit": record.accepted_commit,
            }
        )
        try:
            descriptor = os.open(
                reference, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
            )
        except FileExistsError:
            if reference.is_symlink() or reference.read_bytes() != raw:
                raise ValueError("Private incident reference already exists") from None
        else:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
        async with self.factory() as uow:
            await uow.repair_trials.add_incident(record)
            await uow.commit()
        return record

    async def list_accepted(self) -> tuple[AcceptedIncident, ...]:
        self._directory()
        async with self.factory() as uow:
            records = await uow.repair_trials.list_incidents()
        for record in records:
            case = ApprovedCase.model_validate_json(
                await self._read(record.definition_artifact_id)
            )
            manifest = json.loads(await self._read(record.fixture_manifest_artifact_id))
            if (
                case.case_digest != record.case_digest
                or digest(manifest) != case.case.fixture_manifest_digest
            ):
                raise ValueError(
                    "Private incident definition/fixture identity mismatch"
                )
        return records

    async def discover(self, *, partition: str) -> tuple[AcceptedIncident, ...]:
        if partition not in {"development", "held-out"}:
            raise ValueError("Unknown evaluation partition")
        return await self.list_accepted() if partition == "development" else ()


if TYPE_CHECKING:
    from crucible.application.ports import UnitOfWork

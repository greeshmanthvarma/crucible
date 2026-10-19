"""Semantic trial identities; provenance UUIDs are never equivalence inputs."""

from crucible.benchmarks.admission import grader_policy_digest
from crucible.improvement.contracts import (
    ExperimentManifest,
    FrozenCaseRef,
    HarnessSnapshot,
)
from crucible.repair.identity import digest


def trial_key(
    manifest: ExperimentManifest,
    snapshot: HarnessSnapshot,
    case: FrozenCaseRef,
    repeat_index: int,
) -> str:
    if repeat_index not in range(3):
        raise ValueError("invalid repeat")
    return digest(
        dict(
            case=case,
            source=snapshot.source_digest,
            export=snapshot.export_digest,
            dependencies=manifest.dependency_lock_digest,
            controller=manifest.controller_digest,
            worker=manifest.worker_image,
            platform=manifest.worker_platform,
            relay=manifest.relay_image,
            protocol=1,
            grader=grader_policy_digest(manifest.grader),
            model=manifest.model,
            limits=manifest.trial_limits,
            commands=manifest.command_policy,
            authorization=manifest.disposable_commands_authorized,
            network=manifest.network_policy,
            scoring=manifest.scoring_version,
            suite=manifest.boundary_suite_digest,
            repeat=repeat_index,
        )
    )

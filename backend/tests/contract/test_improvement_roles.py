"""Role job authority is distinct from trials and editable model output."""

from uuid import uuid4

import pytest

from crucible.improvement.worker_protocol import RoleJob
from tests.unit.test_improvement_search_store import search_manifest


def role_job(kind="mining"):
    manifest = search_manifest()
    return RoleJob(
        schema_version=2,
        protocol_version=2,
        job_id=uuid4(),
        role_id=uuid4(),
        round_id=uuid4(),
        experiment_id=manifest.experiment_id,
        job_kind=kind,
        harness_digest="a" * 64,
        export_digest="b" * 64,
        model=manifest.model,
        limits=manifest.role_limits["proposal" if kind == "proposal" else "mining"],
        input_digest="c" * 64,
        workspace="/workspace",
        command_image=manifest.worker_image,
        command_platform=manifest.worker_platform,
        output_limit_bytes=1024,
        wall_deadline_seconds=90,
    )


def test_role_job_excludes_trial_and_hidden_fields():
    job = role_job()
    data = job.model_dump()
    assert "solver_case" not in data
    assert "trial_id" not in data
    assert "split" not in data
    with pytest.raises(ValueError):
        RoleJob.model_validate({**data, "solver_case": {"case_id": "hidden"}})


def test_mining_registry_contains_only_read_tools():
    from crucible.improvement.role_worker import role_registry

    registry = role_registry("mining")
    assert {tool.name for tool in registry.definitions} == {
        "list_files",
        "search_files",
        "read_file",
        "read_file_chunk",
    }
    for tool in ("write_file", "apply_patch", "execute_command"):
        assert registry.get(tool) is None


def test_role_job_cannot_increase_frozen_limits():
    from crucible.improvement.worker_protocol import validate_job_manifest

    job = role_job()
    manifest = search_manifest().model_copy(
        update={"experiment_id": job.experiment_id, "model": job.model}
    )
    validate_job_manifest(job, manifest)
    with pytest.raises(ValueError, match="limits|manifest"):
        validate_job_manifest(
            job.model_copy(
                update={
                    "limits": job.limits.model_copy(update={"max_model_tokens": 101})
                }
            ),
            manifest,
        )


def test_proposal_command_image_is_bound_to_frozen_manifest():
    from crucible.improvement.worker_protocol import validate_job_manifest

    job = role_job("proposal")
    manifest = search_manifest().model_copy(update={"experiment_id": job.experiment_id})
    assert job.command_image == manifest.worker_image
    with pytest.raises(ValueError, match="environment"):
        validate_job_manifest(
            job.model_copy(update={"command_image": "other@sha256:" + "f" * 64}),
            manifest,
        )


def test_role_runtime_export_contains_import_closure():
    from crucible.improvement.source import solver_runtime_path

    for name in (
        "improvement/role_worker.py",
        "improvement/worker.py",
        "improvement/worker_protocol.py",
        "application/worker_container.py",
        "tools/registry.py",
        "tools/filesystem.py",
    ):
        assert solver_runtime_path("backend/src/crucible/" + name)
    for name in ("roles.py", "evidence_views.py", "store.py", "trials.py"):
        assert not solver_runtime_path("backend/src/crucible/improvement/" + name)


async def test_mining_command_request_is_denied_by_host_broker(tmp_path):
    from uuid import uuid4

    from crucible.domain.commands import CommandLimits, CommandNetwork, CommandSpec
    from crucible.improvement.broker import WorkspaceBinding
    from crucible.repair.gateway import GatewayDenied
    from tests.unit.test_improvement_broker import setup_broker

    store, broker, old, credential = await setup_broker(tmp_path, None)
    try:
        original = broker.sessions[old.job_id].manifest
        manifest = original.model_copy(
            update={"schema_version": 2, "worker_protocol_version": 2}
        )
        job = role_job().model_copy(
            update={
                "experiment_id": manifest.experiment_id,
                "command_image": manifest.worker_image,
                "model": manifest.model,
                "limits": manifest.role_limits["mining"],
            }
        )
        token = broker.issue(
            job,
            manifest,
            WorkspaceBinding(tmp_path, manifest.worker_image, manifest.worker_platform),
        )
        spec = CommandSpec(
            "python",
            ("-V",),
            ".",
            1,
            CommandNetwork.NONE,
            {},
            manifest.worker_image,
            "probe",
            CommandLimits(1, 1024, 1, 16),
        )
        with pytest.raises(GatewayDenied, match="mining commands forbidden"):
            await broker.command(token.token, job.job_id, uuid4(), spec)
        assert broker.sessions[job.job_id].command_calls == 0
    finally:
        await store.close()


def test_role_observation_wrong_role_is_rejected():
    from crucible.improvement.worker_protocol import (
        RoleObservation,
        read_role_observation,
    )

    job = role_job()
    observation = RoleObservation(
        schema_version=2,
        job_id=job.job_id,
        role_id=uuid4(),
        round_id=job.round_id,
        experiment_id=job.experiment_id,
        export_digest=job.export_digest,
        termination="completed",
        assistant_output_ref="assistant.txt",
        trace_ref="trace.json",
        detail_code=None,
    )
    with pytest.raises(ValueError, match="identity"):
        read_role_observation(observation.model_dump_json().encode(), job)


def test_mining_workspace_is_read_only_at_container_boundary(tmp_path):
    from crucible.improvement.worker_supervisor import worker_container_arguments

    job = role_job()
    manifest = search_manifest()
    args = worker_container_arguments(
        job,
        manifest,
        network="owned-network",
        harness=tmp_path / "harness",
        workspace=tmp_path / "workspace",
        state=tmp_path / "state",
        job_root=tmp_path / "job",
        driver=tmp_path / "launch.py",
    )
    assert (
        f"type=bind,source={tmp_path / 'workspace'},target=/workspace,readonly" in args
    )
    assert f"type=bind,source={tmp_path / 'harness'},target=/harness,readonly" in args

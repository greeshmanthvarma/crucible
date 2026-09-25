import json
from pathlib import Path

from crucible.evals.cli import run_cli
from crucible.evals.fake_gateway import DeterministicEvalGateway
from crucible.sandbox.docker_client import ContainerInfo, VolumeInfo


class FakeDocker:
    def __init__(self) -> None:
        self.volumes: dict[str, VolumeInfo] = {}
        self.eval_mounts: list[tuple[str, ...]] = []

    async def create_volume(self, name, labels):
        value = VolumeInfo(name, labels)
        self.volumes[name] = value
        return value

    async def inspect_volume(self, identity):
        return self.volumes.get(identity)

    async def list_volumes(self, label):
        return tuple(self.volumes.values())

    async def list_containers(self, label):
        return ()

    async def create_container(self, arguments):
        self.eval_mounts.append(arguments)
        return f"fake-eval-{len(self.eval_mounts)}"

    async def start_attached(self, container_id, on_chunk):
        return 0

    async def inspect_container(self, container_id):
        return ContainerInfo(container_id, "sha256:" + "a" * 64, False, {})

    async def stop_container(self, container_id, grace_seconds):
        return None

    async def kill_container(self, container_id):
        return None

    async def remove_container(self, container_id):
        return None


async def test_smoke_suite_reuses_harness_with_fresh_repeated_trials(
    tmp_path: Path, capsys
) -> None:
    docker = FakeDocker()
    status = await run_cli(
        [
            "eval",
            "run",
            "smoke",
            "--deterministic",
            "--trials",
            "2",
            "--data-dir",
            str(tmp_path),
        ],
        docker_client=docker,
    )
    assert status == 0
    summary = json.loads(capsys.readouterr().out)
    trials = summary["trials"]
    assert len(trials) == 6
    assert all(item["verdict"] == "passed" for item in trials)
    assert len({item["task_id"] for item in trials}) == 6
    assert len({item["run_id"] for item in trials}) == 6
    assert len({item["report_artifact_id"] for item in trials}) == 6
    reports = [json.loads(Path(item["report_path"]).read_text()) for item in trials]
    assert {item["fixture_commit"] for item in reports} == {
        "109b100bb64337098d19b75ba8127cd11762ffff",
        "89571d6695cc26697de1caf1652a1e0a919e1ad5",
        "ac35176695fd537631d75ae6f0f3d9b876158d34",
    }
    assert all(item["files_changed"] == 1 for item in reports)
    assert all(item["raw_trace_ids"]["events"] for item in reports)
    assert all(item["estimated_cost"] is None for item in reports)
    assert len(docker.eval_mounts) == 4
    assert all(
        any("dst=/tests,readonly" in part for part in args)
        for args in docker.eval_mounts
    )
    assert all(
        not any("dst=/workspace" in part for part in args)
        for args in docker.eval_mounts
    )
    assert DeterministicEvalGateway is not None

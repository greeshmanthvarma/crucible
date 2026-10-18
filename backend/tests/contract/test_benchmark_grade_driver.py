"""Driver policy contracts; no Docker, evaluator install, or task data required."""

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest


def test_driver_enforces_docker_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    script = Path(__file__).parents[2] / "scripts/swebench_grade_driver.py"
    spec = importlib.util.spec_from_file_location("benchmark_driver", script)
    assert spec and spec.loader
    driver = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(driver)
    monkeypatch.setattr(driver.resource, "setrlimit", lambda *args: None)
    monkeypatch.setattr(driver.signal, "signal", lambda *args: None)
    monkeypatch.setattr(driver.os, "chdir", lambda path: None)
    source = tmp_path / "source"
    source.mkdir()
    output = tmp_path / "out"
    output.mkdir()
    patch = tmp_path / "patch.diff"
    patch.write_bytes(b"")
    task = tmp_path / "task.json"
    task.write_text(
        json.dumps(
            dict(
                instance_id="case",
                image="image@sha256:" + "a" * 64,
                repo="org/repo",
                base_commit="a" * 40,
            )
        )
    )
    request = dict(
        schema_version=1,
        case_id="case",
        evaluation_id="evaluation",
        trial_id="trial",
        task_input=str(task),
        patch=str(patch),
        task_digest=driver.hashlib.sha256(task.read_bytes()).hexdigest(),
        patch_digest=driver.hashlib.sha256(b"").hexdigest(),
        image="image@sha256:" + "a" * 64,
        platform="linux/amd64",
        repo="org/repo",
        base_commit="a" * 40,
        config=dict(
            driver_commit=driver.PIN,
            driver_source=str(source),
            memory_bytes=1024,
            cpus=2,
            timeout_seconds=10,
            output_limit_bytes=65536,
            network="none",
        ),
    )
    request_path = output / "request.json"
    request_path.write_text(json.dumps(request))
    seen = {}

    class Container:
        attrs = {
            "HostConfig": {
                "Memory": 1024,
                "NanoCpus": 2_000_000_000,
                "NetworkMode": "none",
            },
            "Config": {"Labels": {"crucible.benchmark.evaluation": "evaluation"}},
        }

        def reload(self):
            pass

        def remove(self, **kwargs):
            seen["removed"] = True

    def create(**kwargs):
        seen.update(kwargs)
        return Container()

    docker = ModuleType("docker")
    docker.from_env = lambda **kwargs: SimpleNamespace(
        images=SimpleNamespace(
            get=lambda name: SimpleNamespace(attrs={"Architecture": "amd64"})
        ),
        containers=SimpleNamespace(create=create),
        close=lambda: None,
    )
    docker.errors = SimpleNamespace(NotFound=type("NotFound", (Exception,), {}))
    official = ModuleType("swebench.harness.run_evaluation")
    official.__file__ = str(source / "official.py")

    def run_instance(spec, prediction, client, run_id, **kwargs):
        assert kwargs["skip_patch"] is True
        client.containers.create(image=request["image"], cap_add=["SYS_ADMIN"])
        return "case", {"case": {"resolved": False}}

    official.run_instance = run_instance
    utils = ModuleType("swebench.harness.utils")
    utils.make_test_spec = lambda task: SimpleNamespace(image_assets={})
    modules = {
        "docker": docker,
        "swebench": ModuleType("swebench"),
        "swebench.harness": ModuleType("swebench.harness"),
        "swebench.harness.run_evaluation": official,
        "swebench.harness.utils": utils,
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.setattr(
        sys,
        "argv",
        [str(script), "--request", str(request_path), "--output", str(output)],
    )
    assert driver.main() == 0
    assert seen["network_mode"] == "none"
    assert seen["mem_limit"] == 1024
    assert seen["nano_cpus"] == 2_000_000_000
    assert seen["platform"] == "linux/amd64"
    assert seen["cap_add"] == []
    assert seen["removed"] is True

"""Driver policy contracts; no Docker, evaluator install, or task data required."""

import errno
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest


@pytest.mark.parametrize("overflow", [False, True])
def test_disconnected_cleanup_preserves_output_verdict(overflow):
    script = Path(__file__).parents[2] / "scripts/swebench_grade_driver.py"
    spec = importlib.util.spec_from_file_location("benchmark_driver_close", script)
    driver = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(driver)

    class DisconnectedStream:
        def __iter__(self):
            yield b"test-output"

        def close(self):
            raise OSError(errno.ENOTCONN, "Socket is not connected")

    budget = driver.OutputBudget(1 if overflow else 11)
    if overflow:
        with pytest.raises(driver.OutputLimitExceeded, match="grading_output_limit"):
            list(budget.stream(DisconnectedStream()))
        assert budget.exceeded
    else:
        assert list(budget.stream(DisconnectedStream())) == [b"test-output"]
        assert not budget.exceeded


def test_unexpected_stream_close_error_is_not_suppressed():
    script = Path(__file__).parents[2] / "scripts/swebench_grade_driver.py"
    spec = importlib.util.spec_from_file_location(
        "benchmark_driver_close_error", script
    )
    driver = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(driver)

    class BrokenStream:
        def __iter__(self):
            return iter(())

        def close(self):
            raise OSError(errno.EIO, "unexpected cleanup failure")

    with pytest.raises(OSError, match="unexpected cleanup failure"):
        list(driver.OutputBudget(100).stream(BrokenStream()))


@pytest.mark.parametrize(
    ("stream_sizes", "catch_error", "limited"),
    [
        ((), True, False),
        ((64,), True, False),
        ((100,), True, True),
        ((32, 33), True, True),
        ((100,), False, True),
    ],
    ids=[
        "no-output",
        "exact-limit",
        "overflow-caught",
        "shared-budget",
        "overflow-raised",
    ],
)
def test_driver_enforces_docker_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stream_sizes: tuple[int, ...],
    catch_error: bool,
    limited: bool,
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

    sizes = iter(stream_sizes)

    def chunks():
        try:
            for _ in range(next(sizes)):
                seen["chunks"] = seen.get("chunks", 0) + 1
                yield b"x" * 1024
        finally:
            seen["stream_closed"] = True

    class Container:
        client = SimpleNamespace(
            api=SimpleNamespace(exec_start=lambda *a, **kw: chunks())
        )
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
        container = client.containers.create(
            image=request["image"], cap_add=["SYS_ADMIN"]
        )
        # Reproduce the upstream accumulator and its exception-catching behavior.
        collected = bytearray()
        try:
            for _ in stream_sizes:
                for chunk in container.client.api.exec_start("exec", stream=True):
                    collected.extend(chunk)
        except Exception:
            if not catch_error:
                raise
        finally:
            seen["collected"] = len(collected)
        return "case", {"case": {"resolved": True}}

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

    if limited:
        assert seen["chunks"] == 65
        assert seen["collected"] == 65536
        assert seen["stream_closed"] is True
        assert json.loads((output / "result.json").read_text())["classification"] == (
            "grading_output_limit"
        )
        assert not (output / "official-report.json").exists()
    else:
        assert json.loads((output / "result.json").read_text())["classification"] == (
            "official_report"
        )
        assert (output / "official-report.json").exists()
        if stream_sizes:
            assert seen["collected"] == 65536
            assert seen["stream_closed"] is True

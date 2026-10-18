"""External interpreter driver for SWE-bench 02e7a74ffd0b707aab73d203fe87bdc7c76afc8e.

Audited upstream harness/run_evaluation.py: run_instance(TestSpec, prediction,
client, run_id, timeout, skip_patch) executes official patch/test/report sequence.
harness/utils.py make_test_spec consumes pre-staged v5 protected task records.
harness/grading.py get_eval_report returns {instance_id: {resolved, patch_exists,
patch_successfully_applied, patch_is_None, infra_failure, tests_status}}.

The injected Docker client enforces pinned pre-staged images, architecture,
network, memory, CPU and ownership. Pull/build/removal of unrelated resources
are unsupported. Test output is byte-counted before upstream accumulation,
with one allowance shared across streams. No inference module or dataset
acquisition is invoked.
A failed patch application produces no complete upstream report and therefore
remains an error, rather than fabricating a grading report or a test failure.
"""

import argparse
import errno
import hashlib
import json
import os
import resource
import signal
import sys
import threading
from pathlib import Path

PIN = "02e7a74ffd0b707aab73d203fe87bdc7c76afc8e"


class OutputLimitExceeded(RuntimeError):
    """Docker output exceeded the evaluation's shared byte allowance."""


class OutputBudget:
    def __init__(self, limit):
        self.remaining = limit
        self.exceeded = False
        self.lock = threading.Lock()

    def stream(self, source):
        try:
            for chunk in source:
                with self.lock:
                    if self.exceeded or len(chunk) > self.remaining:
                        self.exceeded = True
                        raise OutputLimitExceeded("grading_output_limit")
                    self.remaining -= len(chunk)
                # Account before the upstream helper can append to its buffer.
                yield chunk
        finally:
            try:
                source.close()
            except OSError as error:
                # Docker may shut down an already-disconnected socket at EOF.
                # Do not turn successful reads or an output-limit verdict into
                # an unrelated cleanup failure; other OS errors stay visible.
                if error.errno != errno.ENOTCONN:
                    raise


class BoundedAPI:
    def __init__(self, api, budget):
        self.api = api
        self.budget = budget

    def __getattr__(self, name):
        return getattr(self.api, name)

    def exec_start(self, *args, **kwargs):
        if not kwargs.get("stream"):
            raise ValueError("evaluator exec_start must stream output")
        return self.budget.stream(self.api.exec_start(*args, **kwargs))


class BoundedContainer:
    def __init__(self, container, budget):
        self.container = container
        self.client = BoundedClient(container.client, budget)

    def __getattr__(self, name):
        return getattr(self.container, name)


class BoundedClient:
    def __init__(self, client, budget):
        self.client = client
        self.api = BoundedAPI(client.api, budget)

    def __getattr__(self, name):
        return getattr(self.client, name)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    request = json.loads(args.request.read_text())
    config = request["config"]
    if request["schema_version"] != 1 or config["driver_commit"] != PIN:
        raise ValueError("unsupported driver version")
    if (
        config["network"] != "none"
        or min(
            config[key]
            for key in ("memory_bytes", "cpus", "timeout_seconds", "output_limit_bytes")
        )
        <= 0
    ):
        raise ValueError("unsupported resource policy")
    cap = config["output_limit_bytes"]
    resource.setrlimit(resource.RLIMIT_FSIZE, (cap, cap))
    source = Path(config["driver_source"]).resolve()
    sys.path.insert(0, str(source))
    import docker
    import swebench.harness.run_evaluation as official
    from swebench.harness.utils import make_test_spec

    if not Path(official.__file__).resolve().is_relative_to(source):
        raise ValueError("official evaluator import escaped pinned source")
    for key, digest_key in (("task_input", "task_digest"), ("patch", "patch_digest")):
        path = Path(request[key])
        if (
            path.is_symlink()
            or hashlib.sha256(path.read_bytes()).hexdigest() != request[digest_key]
        ):
            raise ValueError("request input integrity failure")
    task = json.loads(Path(request["task_input"]).read_text())
    if task["instance_id"] != request["case_id"] or task["image"] != request["image"]:
        raise ValueError("protected task identity mismatch")
    if task["base_commit"] != request["base_commit"] or task["repo"] != request["repo"]:
        raise ValueError("protected repository identity mismatch")
    spec = make_test_spec(task)
    if spec.image_assets:
        raise ValueError("unstaged multimodal assets unsupported")
    client = docker.from_env(timeout=min(30, config["timeout_seconds"]))
    owned = []
    budget = OutputBudget(cap)
    identity = request["evaluation_id"]
    labels = {
        "crucible.benchmark.evaluation": identity,
        "crucible.benchmark.trial": request["trial_id"],
    }
    image = client.images.get(request["image"])
    expected_arch = request["platform"].split("/")[1]
    if image.attrs["Architecture"] != expected_arch:
        raise ValueError("image architecture mismatch")

    class Images:
        def get(self, image_name):
            if image_name != request["image"]:
                raise ValueError("unpinned image requested")
            return image

        def pull(self, *args, **kwargs):
            raise ValueError("image downloads require separate authorization")

    class Containers:
        def get(self, name):
            # Never look up or remove an existing container by an upstream name.
            raise docker.errors.NotFound("fresh evaluation only")

        def create(self, **kwargs):
            if kwargs.get("image") != request["image"]:
                raise ValueError("image identity mismatch")
            kwargs.update(
                network_mode="none",
                mem_limit=config["memory_bytes"],
                nano_cpus=config["cpus"] * 1_000_000_000,
                platform=request["platform"],
                labels=labels,
                cap_add=[],
                cap_drop=["ALL"],
                pids_limit=256,
                security_opt=["no-new-privileges"],
            )
            container = client.containers.create(**kwargs)
            owned.append(container)
            container.reload()
            host = container.attrs["HostConfig"]
            if (
                host["Memory"] != config["memory_bytes"]
                or host["NanoCpus"] != config["cpus"] * 1_000_000_000
                or host["NetworkMode"] != "none"
            ):
                raise ValueError("Docker resource policy not enforced")
            return BoundedContainer(container, budget)

    class Client:
        images = Images()
        containers = Containers()

    def interrupt(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupt)
    os.chdir(output)
    prediction = {
        "instance_id": request["case_id"],
        "model_name_or_path": identity,
        "model_patch": Path(request["patch"]).read_text(encoding="utf-8"),
    }
    (output / "prediction.jsonl").write_text(json.dumps(prediction) + "\n")
    official.RUN_EVALUATION_LOG_DIR = output / "official"
    envelope = {
        "schema_version": 1,
        "evaluation_id": identity,
        "case_id": request["case_id"],
        "patch_digest": request["patch_digest"],
        "model_name_or_path": identity,
        "classification": "missing_official_report",
    }
    try:
        value = official.run_instance(
            spec,
            prediction,
            Client(),
            identity,
            timeout=config["timeout_seconds"],
            skip_patch=not prediction["model_patch"],
        )
        if budget.exceeded:
            envelope["classification"] = "grading_output_limit"
        elif value is not None:
            instance_id, report = value
            if instance_id != request["case_id"]:
                raise ValueError("official case identity mismatch")
            path = output / "official-report.json"
            path.write_text(json.dumps(report, sort_keys=True))
            envelope.update(
                classification="official_report",
                report_digest=hashlib.sha256(path.read_bytes()).hexdigest(),
            )
    except OutputLimitExceeded:
        envelope["classification"] = "grading_output_limit"
    finally:
        for container in owned:
            try:
                container.reload()
                if (
                    container.attrs["Config"]["Labels"].get(
                        "crucible.benchmark.evaluation"
                    )
                    == identity
                ):
                    container.remove(force=True)
            except docker.errors.NotFound:
                pass
        client.close()
    (output / "result.json").write_text(json.dumps(envelope, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

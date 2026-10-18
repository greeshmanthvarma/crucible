"""Independent grading process with fail-closed provenance and report checks."""

import asyncio
import hashlib
import json
import os
import shutil
import signal
import subprocess
import time
from pathlib import Path
from uuid import UUID

from crucible.benchmarks.contracts import (
    BenchmarkCase,
    GraderConfig,
    GradeResult,
    SolutionPatch,
)
from crucible.benchmarks.manifests import _git, case_digest, fixture_digest

PINNED_COMMIT = "02e7a74ffd0b707aab73d203fe87bdc7c76afc8e"


def evaluation_identity(trial: UUID, case: str, patch: str, driver: str) -> str:
    return hashlib.sha256(f"{trial}:{case}:{patch}:{driver}".encode()).hexdigest()


def validate_official_report(path: Path, case_id: str) -> tuple[bool | None, str]:
    if not path.is_file():
        return None, "missing_official_report"
    try:
        report = json.loads(path.read_text())
        if set(report) != {case_id}:
            return None, "wrong_case_identity"
        result = report[case_id]
        if result.get("infra_failure") is True:
            return None, "official_infrastructure_failure"
        for key in (
            "resolved",
            "patch_exists",
            "patch_successfully_applied",
            "patch_is_None",
            "infra_failure",
        ):
            if type(result.get(key)) is not bool:
                return None, "malformed_official_report"
        if not result["patch_exists"] or not result["patch_successfully_applied"]:
            return None, "incomplete_official_report"
        status = result.get("tests_status")
        if not isinstance(status, dict) or set(status) != {
            "FAIL_TO_PASS",
            "PASS_TO_PASS",
            "FAIL_TO_FAIL",
            "PASS_TO_FAIL",
        }:
            return None, "incomplete_official_report"
        for group in status.values():
            if not isinstance(group, dict) or set(group) != {"success", "failure"}:
                return None, "incomplete_official_report"
            if any(
                not isinstance(values, list)
                or any(not isinstance(x, str) for x in values)
                for values in group.values()
            ):
                return None, "malformed_official_report"
        if (
            not status["FAIL_TO_PASS"]["success"]
            and not status["FAIL_TO_PASS"]["failure"]
        ):
            return None, "incomplete_official_report"
        if result["resolved"] and any(group["failure"] for group in status.values()):
            return None, "inconsistent_official_report"
        return result["resolved"], "official_report"
    except (ValueError, TypeError, KeyError, AttributeError, OSError):
        return None, "malformed_official_report"


def validate_driver_source(config: GraderConfig) -> None:
    source = config.driver_source
    if not source.is_absolute() or any(
        p.is_symlink() for p in [source, *source.parents]
    ):
        raise ValueError("invalid driver source path")
    if (
        config.driver_commit != PINNED_COMMIT
        or _git(source, "rev-parse", "HEAD").decode().strip() != config.driver_commit
        or fixture_digest(source) != config.driver_digest
        or _git(source, "status", "--porcelain", "--untracked-files=all", "--ignored")
    ):
        raise ValueError("driver integrity failure")


class _GradeFailure(ValueError):
    pass


class SwebenchGrader:
    def __init__(self, config: GraderConfig) -> None:
        self.config = config

    async def grade(
        self,
        case: BenchmarkCase,
        patch: SolutionPatch,
        trial_id: UUID,
        output_dir: Path,
    ) -> GradeResult:
        started = time.monotonic()
        config = self.config
        identity = evaluation_identity(
            trial_id, case_digest(case), patch.sha256, config.driver_digest
        )
        if output_dir.exists() or any(
            p.is_symlink() for p in [output_dir, *output_dir.parents]
        ):
            raise ValueError("grade output must be fresh and controller owned")
        output_dir.mkdir(parents=True, mode=0o700)
        log = output_dir / "driver.log"
        log.touch(mode=0o600)

        def result(
            code: str, resolved: bool | None = None, report: Path | None = None
        ) -> GradeResult:
            return GradeResult(
                "error" if resolved is None else "pass" if resolved else "fail",
                code,
                identity,
                resolved,
                patch.sha256,
                report,
                log,
                config.driver_commit,
                time.monotonic() - started,
            )

        try:
            for path in (config.driver_python, config.driver_source, patch.path):
                if not path.is_absolute() or any(
                    p.is_symlink() for p in [path, *path.parents]
                ):
                    return result("invalid_driver_or_patch_path")
            if (
                hashlib.sha256(patch.path.read_bytes()).hexdigest() != patch.sha256
                or patch.path.stat().st_size != patch.byte_count
                or patch.base_commit != case.base_commit
            ):
                return result("patch_integrity_failure")
            validate_driver_source(config)
        except (OSError, ValueError, subprocess.CalledProcessError):
            return result("grading_preflight_failure")
        request = {
            "schema_version": 1,
            "case_id": case.case_id,
            "base_commit": case.base_commit,
            "repo": case.repo,
            "evaluation_id": identity,
            "trial_id": str(trial_id),
            "task_input": str(case.grader_input_path),
            "task_digest": case.grader_input_digest,
            "patch": str(patch.path),
            "patch_digest": patch.sha256,
            "image": case.image,
            "platform": case.platform,
            "config": config.model_dump(mode="json"),
        }
        request_path = output_dir / "request.json"
        request_path.write_text(json.dumps(request, sort_keys=True))
        driver = Path(__file__).parents[3] / "scripts/swebench_grade_driver.py"
        env = {"PATH": os.defpath, "HOME": str(output_dir), "PYTHONNOUSERSITE": "1"}
        if "DOCKER_HOST" in os.environ:
            env["DOCKER_HOST"] = os.environ["DOCKER_HOST"]
        try:
            process = await asyncio.create_subprocess_exec(
                str(config.driver_python),
                "-B",
                "-I",
                str(driver),
                "--request",
                str(request_path),
                "--output",
                str(output_dir),
                cwd=output_dir,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,
            )
        except OSError:
            return result("driver_launch_failed")
        assert process.stdout is not None
        retained = 0

        async def drain() -> None:
            nonlocal retained
            assert process.stdout is not None
            with log.open("ab") as stream:
                while chunk := await process.stdout.read(8192):
                    remaining = max(0, config.output_limit_bytes - retained)
                    stream.write(chunk[:remaining])
                    retained += len(chunk)
                    if retained > config.output_limit_bytes:
                        raise OverflowError("driver output limit")

        async def monitor() -> None:
            while process.returncode is None:
                size = sum(
                    p.stat().st_size
                    for p in output_dir.rglob("*")
                    if p.is_file() and not p.is_symlink()
                )
                if size > config.output_limit_bytes + request_path.stat().st_size:
                    raise OverflowError("artifact output limit")
                await asyncio.sleep(0.02)

        drain_task = asyncio.create_task(drain())
        monitor_task = asyncio.create_task(monitor())
        wait_task = asyncio.create_task(process.wait())
        code = "driver_process_failed"
        resolved_out: bool | None = None
        report_out: Path | None = None
        try:
            async with asyncio.timeout(config.timeout_seconds):
                await asyncio.gather(wait_task, drain_task, monitor_task)
            if process.returncode == 0:
                envelope = json.loads((output_dir / "result.json").read_text())
                if (
                    envelope.get("evaluation_id") != identity
                    or envelope.get("patch_digest") != patch.sha256
                    or envelope.get("case_id") != case.case_id
                    or envelope.get("model_name_or_path") != identity
                ):
                    raise _GradeFailure("driver_identity_mismatch")
                if envelope.get("classification") != "official_report":
                    raise _GradeFailure(
                        str(envelope.get("classification", "missing_official_report"))
                    )
                report = output_dir / "official-report.json"
                if report.is_symlink() or hashlib.sha256(
                    report.read_bytes()
                ).hexdigest() != envelope.get("report_digest"):
                    raise _GradeFailure("report_integrity_failure")
                resolved, code = validate_official_report(report, case.case_id)
                if resolved is not None:
                    task_input = json.loads(case.grader_input_path.read_text())
                    status = json.loads(report.read_text())[case.case_id][
                        "tests_status"
                    ]
                    for group in ("FAIL_TO_PASS", "PASS_TO_PASS"):
                        expected = task_input[group]
                        if isinstance(expected, str):
                            expected = json.loads(expected)
                        observed = status[group]["success"] + status[group]["failure"]
                        if len(observed) != len(set(observed)) or set(observed) != set(
                            expected
                        ):
                            raise _GradeFailure("incomplete_official_report")
                resolved_out = resolved
                report_out = report if resolved is not None else None
        except asyncio.CancelledError:
            code = "grading_cancelled"
            raise
        except TimeoutError:
            code = "grading_timeout"
        except OverflowError:
            code = "grading_output_limit"
        except _GradeFailure as error:
            code = str(error)
        except (OSError, ValueError, TypeError, KeyError):
            code = "missing_official_report"
        finally:
            if process.returncode is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    await asyncio.wait_for(process.wait(), 5)
                except TimeoutError:
                    os.killpg(process.pid, signal.SIGKILL)
                    await process.wait()
            try:
                await self._cleanup_owned(identity, str(trial_id), env)
            except (OSError, RuntimeError, TimeoutError):
                code = "grading_cleanup_failed"
                resolved_out = None
                report_out = None
            for task in (drain_task, monitor_task, wait_task):
                task.cancel()
            await asyncio.gather(
                drain_task, monitor_task, wait_task, return_exceptions=True
            )
        return result(code, resolved_out, report_out)

    async def _cleanup_owned(
        self, evaluation: str, trial: str, env: dict[str, str]
    ) -> None:
        docker = shutil.which("docker")
        if docker is None:
            raise RuntimeError("owned grading container cleanup unavailable")
        process = await asyncio.create_subprocess_exec(
            docker,
            "ps",
            "-aq",
            "--filter",
            f"label=crucible.benchmark.evaluation={evaluation}",
            "--filter",
            f"label=crucible.benchmark.trial={trial}",
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            stdout, _ = await asyncio.wait_for(process.communicate(), 20)
        except BaseException:
            process.kill()
            await process.wait()
            raise
        if process.returncode != 0:
            raise RuntimeError("owned grading container cleanup lookup failed")
        for container in stdout.decode().splitlines():
            if not container or any(c not in "0123456789abcdef" for c in container):
                raise RuntimeError("invalid owned container identity")
            removal = await asyncio.create_subprocess_exec(
                docker,
                "rm",
                "-f",
                container,
                env=env,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            try:
                await asyncio.wait_for(removal.wait(), 20)
            except BaseException:
                removal.kill()
                await removal.wait()
                raise
            if removal.returncode != 0:
                raise RuntimeError("owned grading container removal failed")

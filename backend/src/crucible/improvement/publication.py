"""Optional, intent-first publication of an already frozen report bundle."""

import asyncio
import hashlib
import json
import re
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Literal, Protocol
from uuid import UUID, uuid4

from pydantic import Field

from crucible.benchmarks.contracts import Commit, Sha256
from crucible.benchmarks.evidence import atomic_json
from crucible.improvement.contracts import Contract, ExperimentPaths
from crucible.improvement.reporting import ReportBundle, verify_bundle
from crucible.improvement.source import atomic_source_file, git_bytes, safe_path
from crucible.improvement.store import ExperimentStore, RevisionConflict
from crucible.improvement.worker_supervisor import read_regular_artifact
from crucible.repair.identity import digest


class PublicationRecord(Contract):
    experiment_id: UUID
    seal_digest: Sha256
    report_digest: Sha256
    branch: Annotated[str, Field(max_length=100)]
    expected_head: Commit
    base_branch: Annotated[str, Field(max_length=200)]
    remote_url: Annotated[str, Field(max_length=2048)]
    pr_url: Annotated[str, Field(max_length=2048)] | None = None
    pr_number: Annotated[int, Field(gt=0)] | None = None
    status: Literal["intent", "pushed", "labeling", "published"] = "intent"
    last_error: Literal["publication_failed"] | None = None


class ImprovementPR(Contract):
    number: Annotated[int, Field(gt=0)]
    url: Annotated[str, Field(max_length=2048)]
    state: Literal["open", "closed"]
    head_sha: Commit
    head_branch: Annotated[str, Field(max_length=100)]
    base_branch: Annotated[str, Field(max_length=200)]
    ownership_digest: Sha256 | None
    labels: Annotated[tuple[str, ...], Field(max_length=100)]


class PublicationPort(Protocol):
    async def remote_url(self, remote: str) -> str: ...
    async def remote_head(self, remote: str, branch: str) -> str | None: ...
    async def push_expected_branch(
        self, remote: str, branch: str, head: str, expected: str | None
    ) -> None: ...
    async def find_pr(self, branch: str, base: str) -> ImprovementPR | None: ...
    async def create_pr(
        self, branch: str, base: str, title: str, body: str
    ) -> ImprovementPR: ...
    async def ensure_label(self, number: int, label: str) -> None: ...


def ownership_digest(record: PublicationRecord) -> str:
    return digest(
        record.model_dump(exclude={"pr_url", "pr_number", "status", "last_error"})
    )


def owns_publication_pr(
    record: PublicationRecord, pr: dict[str, object], repository: str
) -> bool:
    """Read-only admission exception for an exact already-intended publication."""
    head, base = pr.get("head"), pr.get("base")
    if not isinstance(head, dict) or not isinstance(base, dict):
        return False
    head_repo, base_repo = head.get("repo"), base.get("repo")
    if not isinstance(head_repo, dict) or not isinstance(base_repo, dict):
        return False
    return bool(
        head.get("ref") == record.branch
        and head.get("sha") == record.expected_head
        and head_repo.get("full_name") == repository
        and base.get("ref") == record.base_branch
        and base_repo.get("full_name") == repository
        and pr.get("state") == "open"
        and f"Crucible publication: {ownership_digest(record)}"
        in str(pr.get("body") or "").splitlines()
        and (
            record.pr_number is None
            or (pr.get("number"), pr.get("html_url"))
            == (record.pr_number, record.pr_url)
        )
    )


class PublicationService:
    def __init__(
        self, store: ExperimentStore, paths: ExperimentPaths, port: PublicationPort
    ) -> None:
        self.store, self.paths, self.port = store, paths, port

    async def _report_commit(
        self, bundle: ReportBundle, directory: Path, branch: str
    ) -> str:
        verify_bundle(bundle, directory)
        seal = await self.store.search_seal(bundle.experiment_id)
        assert seal is not None
        root = self.paths.root / "publication" / str(bundle.experiment_id)
        safe_path(root)
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        intent_path = root / "export-intent.json"
        identity = {
            "experiment_id": str(bundle.experiment_id),
            "report_digest": bundle.report_digest,
            "code_commit": seal.selected_final_commit,
            "branch": branch,
        }
        if intent_path.exists():
            intent = json.loads(read_regular_artifact(intent_path, 65536))
            if any(intent.get(k) != v for k, v in identity.items()):
                raise RevisionConflict("report export intent changed")
        else:
            intent = {
                **identity,
                "timestamp": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            }
            atomic_json(intent_path, intent)
        export = root / "worktree"
        if not export.exists():
            await asyncio.to_thread(
                git_bytes,
                self.paths.repository,
                "worktree",
                "add",
                "--detach",
                str(export),
                seal.selected_final_commit,
            )
        registered = (
            git_bytes(self.paths.repository, "worktree", "list", "--porcelain")
            .decode()
            .split("\n\n")
        )
        if export.is_symlink() or not any(
            {f"worktree {export}", f"HEAD {seal.selected_final_commit}", "detached"}
            <= set(entry.splitlines())
            for entry in registered
        ):
            raise RevisionConflict("report worktree ownership differs")
        parent_tree = (
            git_bytes(export, "rev-parse", seal.selected_final_commit + "^{tree}")
            .decode()
            .strip()
        )
        if git_bytes(export, "write-tree").decode().strip() != parent_tree:
            raise RevisionConflict("report index has unexpected staged edits")
        prefix = f"reports/self-improvement/{bundle.experiment_id}/"
        allowed = {prefix + name for name in bundle.files}
        changed = set(
            filter(
                None,
                git_bytes(
                    export,
                    "ls-files",
                    "--modified",
                    "--deleted",
                    "--others",
                    "--exclude-standard",
                    "-z",
                )
                .decode()
                .split("\0"),
            )
        )
        if not changed <= allowed:
            raise RevisionConflict("unexpected report export worktree edits")
        for name, sha in bundle.files.items():
            path = export / prefix / name
            safe_path(path)
            if (
                path.exists()
                and hashlib.sha256(
                    read_regular_artifact(path, 32 * 1024 * 1024)
                ).hexdigest()
                != sha
            ):
                raise RevisionConflict("owned report export file changed")
            if not path.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                atomic_source_file(
                    path,
                    (
                        0o100644,
                        read_regular_artifact(directory / name, 32 * 1024 * 1024),
                    ),
                    root / "file-staging",
                )
        index = root / ("index-" + str(uuid4()))
        try:
            git_bytes(export, "read-tree", seal.selected_final_commit, index_file=index)
            for name in sorted(allowed):
                raw = read_regular_artifact(export / name, 32 * 1024 * 1024)
                if (
                    hashlib.sha256(raw).hexdigest()
                    != bundle.files[name.removeprefix(prefix)]
                ):
                    raise RevisionConflict("report bytes changed during staging")
                blob = (
                    git_bytes(export, "hash-object", "-w", "--stdin", data=raw)
                    .decode()
                    .strip()
                )
                git_bytes(
                    export,
                    "update-index",
                    "--add",
                    "--cacheinfo",
                    f"100644,{blob},{name}",
                    index_file=index,
                )
            tree = git_bytes(export, "write-tree", index_file=index).decode().strip()
            names = set(
                filter(
                    None,
                    git_bytes(
                        export,
                        "diff-tree",
                        "--no-commit-id",
                        "--name-only",
                        "-r",
                        "-z",
                        parent_tree,
                        tree,
                    )
                    .decode()
                    .split("\0"),
                )
            )
            if names != allowed:
                raise RevisionConflict(
                    "report commit must contain exactly report files"
                )
            return (
                git_bytes(
                    export,
                    "commit-tree",
                    tree,
                    "-p",
                    seal.selected_final_commit,
                    "-m",
                    (
                        f"Export improvement report {bundle.experiment_id}\n\n"
                        f"Evaluated code: {seal.selected_final_commit}\n"
                        f"Report: {bundle.report_digest}"
                    ),
                    commit_date=intent["timestamp"],
                )
                .decode()
                .strip()
            )
        finally:
            index.unlink(missing_ok=True)

    async def publish(
        self,
        id: UUID,
        bundle: ReportBundle,
        directory: Path,
        *,
        remote: str = "origin",
        base: str = "main",
        authorize_remote_write: bool = False,
        report_only: bool = False,
    ) -> PublicationRecord:
        if not authorize_remote_write:
            raise RevisionConflict("explicit remote write authorization required")
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,63}", remote):
            raise ValueError("invalid remote name")
        git_bytes(self.paths.repository, "check-ref-format", "--branch", base)
        seal = await self.store.search_seal(id)
        if (
            seal is None
            or bundle.experiment_id != id
            or bundle.seal_digest != digest(asdict(seal))
        ):
            raise RevisionConflict("publication requires matching sealed report")
        verify_bundle(bundle, directory)
        promoted = bool(await self.store.promotions(id))
        if not promoted and not report_only:
            raise RevisionConflict(
                "no accepted improvement; report-only publication requires "
                "explicit authorization"
            )
        if promoted and report_only:
            raise RevisionConflict("report-only flag differs from accepted improvement")
        branch = f"codex/improve/{id}" if promoted else f"codex/improve-report/{id}"
        existing = await self.store.publication(id)
        remote_url = await self.port.remote_url(remote)
        if existing:
            record = existing
            if (
                record.seal_digest,
                record.report_digest,
                record.branch,
                record.base_branch,
                record.remote_url,
            ) != (bundle.seal_digest, bundle.report_digest, branch, base, remote_url):
                raise RevisionConflict("publication intent changed")
            events = await self.store.events(id, "publication_intent")
            expected = events[-1].get("remote_before")
            if expected is not None and not isinstance(expected, str):
                raise RevisionConflict("publication remote checkpoint differs")
        else:
            if await self.port.find_pr(branch, base) is not None:
                raise RevisionConflict("unrelated pre-existing PR blocks publication")
            head = await self._report_commit(bundle, directory, branch)
            expected = await self.port.remote_head(remote, branch)
            if expected not in (None, seal.selected_final_commit):
                raise RevisionConflict("unowned remote branch")
            record = PublicationRecord(
                experiment_id=id,
                seal_digest=bundle.seal_digest,
                report_digest=bundle.report_digest,
                branch=branch,
                expected_head=head,
                base_branch=base,
                remote_url=remote_url,
            )
            await self.store.prepare_publication(record, expected)

        def validate(pr: ImprovementPR) -> None:
            if (
                pr.head_sha,
                pr.head_branch,
                pr.base_branch,
                pr.state,
                pr.ownership_digest,
            ) != (
                record.expected_head,
                branch,
                base,
                "open",
                ownership_digest(record),
            ) or (
                record.pr_number is not None
                and (pr.number, pr.url) != (record.pr_number, record.pr_url)
            ):
                raise RevisionConflict("unrelated or changed PR cannot be adopted")

        try:
            prior_pr = await self.port.find_pr(branch, base)
            if prior_pr is not None:
                validate(prior_pr)
            actual = await self.port.remote_head(remote, branch)
            if actual != record.expected_head:
                if actual != expected:
                    raise RevisionConflict("remote branch advanced unexpectedly")
                await self.port.push_expected_branch(
                    remote, branch, record.expected_head, expected
                )
            if await self.port.remote_head(remote, branch) != record.expected_head:
                raise RevisionConflict("remote push not confirmed")
            record = record.model_copy(update={"status": "pushed", "last_error": None})
            await self.store.save_publication(record)
            pr = await self.port.find_pr(branch, base)
            if pr is None:
                title = (
                    "Measured harness improvement"
                    if promoted
                    else "Benchmark report: no accepted harness improvement"
                )
                completeness = "complete" if bundle.complete else "INCONCLUSIVE"
                body = (
                    f"{title}.\n\nEvaluated code: `{seal.selected_final_commit}`.\n"
                    f"Report: `reports/self-improvement/{id}/report.md`.\n"
                    f"Seal: `{bundle.seal_digest}`.\n\n"
                    f"Crucible publication: {ownership_digest(record)}\n\n"
                    f"Measurement is {completeness}. "
                    "Merge and restart remain operator actions."
                )
                pr = await self.port.create_pr(branch, base, title, body)

            validate(pr)
            record = record.model_copy(
                update={"status": "labeling", "pr_number": pr.number, "pr_url": pr.url}
            )
            await self.store.save_publication(record)
            await self.port.ensure_label(pr.number, "crucible-improvement")
            checked = await self.port.find_pr(branch, base)
            if checked is None:
                raise RevisionConflict("published PR unavailable")
            validate(checked)
            if (
                "crucible-improvement" not in checked.labels
                or await self.port.remote_head(remote, branch) != record.expected_head
            ):
                raise RevisionConflict("publication label/head not confirmed")
            record = record.model_copy(update={"status": "published"})
            await self.store.save_publication(record)
            return record
        except Exception:
            await self.store.save_publication(
                record.model_copy(update={"last_error": "publication_failed"})
            )
            raise


class LocalPublicationPort:
    """Normal non-force Git push and exact-repository GitHub CLI operations."""

    def __init__(self, root: Path, remote: str = "origin") -> None:
        self.root, self.remote = root, remote

    async def _run(self, *argv: str) -> str:
        from crucible.improvement.lifecycle import bounded_process

        return await bounded_process(self.root, *argv, limit=2 * 1024 * 1024)

    async def remote_url(self, remote: str) -> str:
        return (
            (await asyncio.to_thread(git_bytes, self.root, "remote", "get-url", remote))
            .decode()
            .strip()
        )

    async def remote_head(self, remote: str, branch: str) -> str | None:
        raw = await self._run(
            "git", "ls-remote", "--refs", remote, "refs/heads/" + branch
        )
        records = [line.split() for line in raw.splitlines() if line]
        if not records:
            return None
        if (
            len(records) != 1
            or records[0][1] != "refs/heads/" + branch
            or not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", records[0][0])
        ):
            raise RevisionConflict("remote identity ambiguous")
        return records[0][0]

    async def push_expected_branch(
        self, remote: str, branch: str, head: str, expected: str | None
    ) -> None:
        if await self.remote_head(remote, branch) != expected:
            raise RevisionConflict("remote changed before non-force push")
        await self._run(
            "git", "push", "--no-verify", remote, f"{head}:refs/heads/{branch}"
        )
        if await self.remote_head(remote, branch) != head:
            raise RevisionConflict("remote push identity differs")

    async def _repository(self) -> str:
        from crucible.repair.admission import LocalRepairAdmissionGitHub

        return LocalRepairAdmissionGitHub._repository(
            await self.remote_url(self.remote)
        )

    async def find_pr(self, branch: str, base: str) -> ImprovementPR | None:
        # Find every use of the head, including PRs targeting a different base.
        repo = await self._repository()
        raw = await self._run(
            "gh",
            "api",
            "--method",
            "GET",
            f"repos/{repo}/pulls",
            "-f",
            "state=all",
            "-f",
            f"head={repo.split('/')[0]}:{branch}",
            "-f",
            "per_page=100",
        )
        records = json.loads(raw)
        if len(records) > 1:
            raise RevisionConflict("ambiguous improvement PR")
        if not records:
            return None
        pr = records[0]
        if (
            pr["head"]["repo"]["full_name"] != repo
            or pr["base"]["repo"]["full_name"] != repo
        ):
            raise RevisionConflict("PR repository differs")
        markers = re.findall(
            r"^Crucible publication: ([0-9a-f]{64})$",
            pr.get("body") or "",
            re.MULTILINE,
        )
        return ImprovementPR(
            number=pr["number"],
            url=pr["html_url"],
            state=pr["state"],
            head_sha=pr["head"]["sha"],
            head_branch=pr["head"]["ref"],
            base_branch=pr["base"]["ref"],
            ownership_digest=markers[0] if len(markers) == 1 else None,
            labels=tuple(label["name"] for label in pr["labels"]),
        )

    async def create_pr(
        self, branch: str, base: str, title: str, body: str
    ) -> ImprovementPR:
        repo = await self._repository()
        with TemporaryDirectory(prefix="improvement-pr-") as directory:
            path = Path(directory) / "body.md"
            path.write_text(body)
            path.chmod(0o600)
            await self._run(
                "gh",
                "pr",
                "create",
                "--repo",
                repo,
                "--head",
                branch,
                "--base",
                base,
                "--title",
                title,
                "--body-file",
                str(path),
            )
        pr = await self.find_pr(branch, base)
        if pr is None:
            raise RevisionConflict("created PR identity unavailable")
        return pr

    async def ensure_label(self, number: int, label: str) -> None:
        repo = await self._repository()
        await self._run(
            "gh",
            "label",
            "create",
            label,
            "--repo",
            repo,
            "--color",
            "5319e7",
            "--force",
        )
        await self._run(
            "gh", "pr", "edit", str(number), "--repo", repo, "--add-label", label
        )

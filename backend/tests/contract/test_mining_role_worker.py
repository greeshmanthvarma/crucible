"""Protected diagnostic worker capabilities and successful-read evidence."""

from uuid import UUID, uuid4

import pytest

from crucible.improvement.mining_inspection import audited_role_registry
from crucible.tools.definitions import ToolContext


async def test_analyst_cannot_read_sibling_trial_or_reports(tmp_path):
    for path in (
        "trial/own.txt",
        "trials/sibling/secret.txt",
        "reports/sibling.json",
        "source/code.py",
    ):
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("visible")
    receipts = []
    tool = audited_role_registry("trial_analysis", receipts, UUID(int=1)).get(
        "read_file_chunk"
    )
    context = ToolContext(tmp_path, tool_call_id=uuid4())
    for path in (
        "trials/sibling/secret.txt",
        "reports/sibling.json",
        "../private",
        "/private/key",
    ):
        with pytest.raises(ValueError):
            await tool.invoke(context, dict(path=path))
    assert not receipts
    assert (
        await tool.invoke(context, dict(path="trial/own.txt"))
    ).display_text == "visible"
    assert len(receipts) == 1


@pytest.mark.parametrize(
    "kind", ["trial_analysis", "mining_synthesis", "format_correction"]
)
def test_diagnostic_roles_have_no_command_authority(kind):
    registry = audited_role_registry(kind, [], uuid4())
    assert not any(
        registry.get(name)
        for name in (
            "execute_command",
            "write_file",
            "apply_patch",
            "workspace_diff",
            "workspace_status",
        )
    )
    if kind == "format_correction":
        assert registry.definitions == ()


async def test_read_receipts_cover_only_successful_returned_bytes(tmp_path):
    import hashlib

    (tmp_path / "trial").mkdir()
    raw = "one\nαβγ\nlast\n".encode()
    (tmp_path / "trial" / "text").write_bytes(raw)
    (tmp_path / "trial" / "binary").write_bytes(b"\x00bad")
    receipts = []
    registry = audited_role_registry("trial_analysis", receipts, UUID(int=1))
    context = ToolContext(tmp_path, tool_call_id=uuid4())
    chunk = registry.get("read_file_chunk")
    outcome = await chunk.invoke(context, dict(path="trial/text", offset=4, limit=5))
    assert outcome.display_text == "αβ"
    assert outcome.data["next_offset"] == 8
    assert (receipts[0].start_byte, receipts[0].end_byte) == (4, 8)
    assert receipts[0].sha256 == hashlib.sha256(raw).hexdigest()
    await registry.get("read_file").invoke(
        context, dict(path="trial/text", start_line=3, end_line=3)
    )
    assert (receipts[1].start_byte, receipts[1].end_byte) == (11, 16)
    assert (
        await chunk.invoke(context, dict(path="trial/binary"))
    ).error_code == "binary_file"
    with pytest.raises(UnicodeDecodeError):
        await chunk.invoke(context, dict(path="trial/text", offset=5, limit=4))
    await registry.get("list_files").invoke(context, dict(path="trial"))
    assert len(receipts) == 2


async def test_alias_symlinks_cannot_bypass_positive_view(tmp_path):
    (tmp_path / "trial").mkdir()
    (tmp_path / "reports").mkdir()
    (tmp_path / "reports" / "private").write_text("sibling")
    (tmp_path / "trial" / "alias").symlink_to(tmp_path / "reports" / "private")
    tool = audited_role_registry("trial_analysis", [], uuid4()).get("read_file")
    with pytest.raises(ValueError, match="symlink"):
        await tool.invoke(
            ToolContext(tmp_path, tool_call_id=uuid4()), dict(path="trial/alias")
        )
    root_list = (
        await audited_role_registry("trial_analysis", [], uuid4())
        .get("list_files")
        .invoke(ToolContext(tmp_path), dict(path="."))
    )
    assert "reports" not in root_list.data["paths"]


@pytest.mark.parametrize("fault", ["job", "hash", "size", "path"])
def test_receipt_artifacts_reject_forged_identity_or_bounds(fault):
    import hashlib

    from crucible.improvement.mining_contracts import InspectionReceipt
    from crucible.improvement.mining_inspection import read_inspection_receipts
    from crucible.repair.identity import canonical_json

    receipt = InspectionReceipt(
        job_id=UUID(int=1),
        path="trial/file",
        sha256="a" * 64,
        start_byte=0,
        end_byte=4,
        tool_call_id=uuid4(),
    )
    raw = canonical_json((receipt,))
    expected = hashlib.sha256(raw).hexdigest()
    job = UUID(int=1)
    if fault == "job":
        job = UUID(int=2)
    elif fault == "hash":
        expected = "f" * 64
    elif fault == "size":
        raw += b" " * 1048576
        expected = hashlib.sha256(raw).hexdigest()
    else:
        raw = raw.replace(b"trial/file", b"reports/sibling")
        expected = hashlib.sha256(raw).hexdigest()
    with pytest.raises(ValueError):
        read_inspection_receipts(
            raw, job_id=job, expected_sha256=expected, kind="trial_analysis"
        )


def worker_job(tmp_path, kind="trial_analysis"):
    import hashlib
    import json

    from crucible.improvement.contracts import MiningDiagnosticManifest
    from crucible.improvement.worker_protocol import RoleJob
    from tests.contract.test_mining_diagnostic import diagnostic_payload, role_payload

    metadata = diagnostic_payload()
    metadata["model"]["input_limit"] = 20000
    manifest = MiningDiagnosticManifest.model_validate_json(json.dumps(metadata))
    payload = role_payload(manifest, kind)
    if kind == "format_correction":
        payload.update(
            correction_of_job_id=str(uuid4()), correction_target="trial_analysis"
        )
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "trial").mkdir()
    (workspace / "source").mkdir()
    (workspace / "trial" / "read.txt").write_text("original record bytes")
    raw = json.dumps(
        dict(
            output_schema={"type": "object"},
            original_output="{}",
            validation_errors=["invalid_report"],
        )
        if kind == "format_correction"
        else dict(
            overview="trial/overview.json",
            index="trial/trajectory.jsonl",
            output_schema={"type": "object"},
        )
    ).encode()
    (workspace / "input.json").write_bytes(raw)
    payload["input_digest"] = hashlib.sha256(raw).hexdigest()
    job = RoleJob.model_validate_json(json.dumps(payload))
    job_path = tmp_path / "job.json"
    job_path.write_text(job.model_dump_json())
    state = tmp_path / "state"
    state.mkdir()
    return job, workspace, state, job_path


@pytest.mark.parametrize("fault", [None, "quota", "provider"])
async def test_host_quota_denial_is_budget_exhausted_not_provider_error(
    tmp_path, monkeypatch, fault
):
    from crucible.engine.gateway import (
        CompleteToolCall,
        ModelError,
        ModelStop,
        ModelStopReason,
        ModelUsage,
        TextDelta,
    )
    from crucible.improvement.mining_inspection import read_inspection_receipts
    from crucible.improvement.role_worker import run_role_worker
    from crucible.improvement.worker_gateway import WorkerModelGateway
    from crucible.improvement.worker_protocol import read_role_observation

    job, workspace, state, job_path = worker_job(tmp_path)
    calls = []

    async def stream(self, request):
        calls.append(request)
        if fault:
            yield ModelError(
                "improvement_budget_exhausted"
                if fault == "quota"
                else "provider_stream_failed",
                "scripted",
            )
            return
        if len(calls) == 1:
            yield CompleteToolCall(
                uuid4(), "read_file_chunk", dict(path="trial/read.txt", limit=8)
            )
            yield CompleteToolCall(uuid4(), "list_files", dict(path="source"))
            yield ModelUsage(10, 10)
            yield ModelStop(ModelStopReason.TOOL_CALLS)
        else:
            yield TextDelta('{"complete":true}')
            yield ModelUsage(10, 10)
            yield ModelStop(ModelStopReason.COMPLETE)

    monkeypatch.setattr(WorkerModelGateway, "stream", stream)
    await run_role_worker(
        job, "scripted-only", workspace=workspace, state_root=state, job_path=job_path
    )
    observation = read_role_observation((state / "observation.json").read_bytes(), job)
    assert (
        observation.termination
        == {
            "quota": "budget_exhausted",
            "provider": "provider_error",
            None: "completed",
        }[fault]
    )
    receipts = read_inspection_receipts(
        (state / observation.inspection_receipts_ref).read_bytes(),
        job_id=job.job_id,
        expected_sha256=observation.inspection_receipts_sha256,
        kind=job.job_kind,
    )
    assert observation.tool_call_count == (0 if fault else 2)
    assert len(receipts) == (0 if fault else 1)
    assert not any(
        t.name == "execute_command" for request in calls for t in request.tools
    )


async def test_format_correction_cannot_read_or_investigate(tmp_path, monkeypatch):
    from crucible.engine.gateway import (
        ModelStop,
        ModelStopReason,
        ModelUsage,
        TextDelta,
    )
    from crucible.improvement.role_worker import run_role_worker
    from crucible.improvement.worker_gateway import WorkerModelGateway
    from crucible.improvement.worker_protocol import read_role_observation

    job, workspace, state, job_path = worker_job(tmp_path, "format_correction")

    async def stream(self, request):
        assert request.tools == ()
        yield TextDelta("{}")
        yield ModelUsage(10, 1)
        yield ModelStop(ModelStopReason.COMPLETE)

    monkeypatch.setattr(WorkerModelGateway, "stream", stream)
    await run_role_worker(
        job, "scripted-only", workspace=workspace, state_root=state, job_path=job_path
    )
    observation = read_role_observation((state / "observation.json").read_bytes(), job)
    assert observation.tool_call_count == 0
    assert (state / "inspection-receipts.json").read_bytes() == b"[]"


def test_diagnostic_runtime_import_graph_excludes_host_authority():
    import ast
    from pathlib import Path

    from crucible.improvement.source import solver_runtime_path

    root = Path(__file__).parents[2] / "src"
    pending = [
        "crucible.improvement.mining_contracts",
        "crucible.improvement.mining_inspection",
    ]
    visited = set()

    class Imports(ast.NodeVisitor):
        def visit_If(self, node):
            if isinstance(node.test, ast.Name) and node.test.id == "TYPE_CHECKING":
                return
            self.generic_visit(node)

        def visit_FunctionDef(self, node):
            return

        def visit_AsyncFunctionDef(self, node):
            return

        def visit_ImportFrom(self, node):
            if node.module and node.module.startswith("crucible."):
                pending.append(node.module)

    while pending:
        module = pending.pop()
        if module in visited:
            continue
        visited.add(module)
        path = root / Path(*module.split(".")).with_suffix(".py")
        if not path.exists():
            path = root / Path(*module.split(".")) / "__init__.py"
        if not path.exists() and path.parent.is_dir():
            continue  # Python namespace package; concrete children are traversed.
        assert path.exists(), module
        assert solver_runtime_path(
            "backend/src/" + path.relative_to(root).as_posix()
        ), module
        Imports().visit(ast.parse(path.read_bytes()))
    assert not any(
        m in visited
        for m in (
            "crucible.improvement.roles",
            "crucible.improvement.store",
            "crucible.improvement.mining_store",
            "crucible.improvement.mining_validation",
            "crucible.improvement.trajectory_export",
        )
    )


async def test_lossy_line_read_does_not_credit_skipped_bytes(tmp_path):
    (tmp_path / "trial").mkdir()
    (tmp_path / "trial" / "invalid").write_bytes(b"ok\xfftext")
    receipts = []
    tool = audited_role_registry("trial_analysis", receipts, uuid4()).get("read_file")
    result = await tool.invoke(
        ToolContext(tmp_path, tool_call_id=uuid4()), dict(path="trial/invalid")
    )
    assert result.display_text == "oktext"  # Preserve the ordinary reader's rendering.
    assert receipts == []


@pytest.mark.parametrize("fault", [None, "failed", "different_display"])
async def test_receipts_credit_only_bounded_successful_model_visible_output(
    tmp_path, fault
):
    from crucible.domain.tools import ToolResultStatus
    from crucible.improvement.mining_inspection import AuditedReadTool
    from crucible.tools.definitions import ReadFileArguments, ToolOutcome
    from crucible.tools.filesystem import ReadFileTool

    (tmp_path / "trial").mkdir()
    text = "x" * 16000
    (tmp_path / "trial" / "file").write_text(text)

    class Reader:
        definition = ReadFileTool().definition
        argument_model = ReadFileArguments
        parallel_safe = True

        async def invoke(self, context, arguments):
            return ToolOutcome(
                {"content": text},
                "unrelated" if fault == "different_display" else text,
                status=ToolResultStatus.FAILED if fault == "failed" else None,
            )

    receipts = []
    wrapped = AuditedReadTool(Reader(), "trial_analysis", receipts, uuid4())
    result = await wrapped.invoke(
        ToolContext(tmp_path, tool_call_id=uuid4()), dict(path="trial/file")
    )
    assert len(result.data["content"].encode()) == 8192
    assert result.truncated
    if fault:
        assert receipts == []
    else:
        assert (receipts[0].start_byte, receipts[0].end_byte) == (0, 8192)

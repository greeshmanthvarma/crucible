"""Controller-created internal Tasks run through the ordinary bounded harness."""

import asyncio
import json
import os
from collections.abc import Callable
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, Literal, TypeAlias, cast
from uuid import UUID, uuid4

from pydantic import Field, ValidationError, model_validator

from crucible.application.artifact_service import ArtifactService
from crucible.application.ports import EventNotifier, UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.conversation import (
    Message,
    MessagePart,
    MessagePartKind,
    MessageRole,
    MessageStatus,
)
from crucible.domain.repair import RepairStatus
from crucible.domain.run import Run, RunStatus
from crucible.domain.task import Task, TaskStatus
from crucible.engine.gateway import ModelToolDefinition
from crucible.engine.run_engine import RunEngine
from crucible.repair.cases import IncidentCase, StrictModel
from crucible.repair.events import append_repair_event
from crucible.repair.identity import canonical_json, digest
from crucible.repair.incident_context import IncidentContext
from crucible.repair.prompts import role_prompt
from crucible.sandbox.resources import TaskResourceManager
from crucible.tools.definitions import Tool, ToolArguments, ToolContext, ToolOutcome
from crucible.tools.registry import ToolRegistry

Role = Literal["diagnosis", "review", "proposal"]
EDITABLE_PATHS = frozenset(
    {
        "backend/src/crucible/tools/filesystem.py",
        "backend/src/crucible/tools/git_tools.py",
        "backend/src/crucible/tools/behavior_contracts.py",
        "backend/src/crucible/context/selection.py",
        "backend/src/crucible/context/summary_behavior.py",
    }
)


class FixItem(StrictModel):
    mechanism: str = Field(min_length=1)
    decision: Literal["repairable", "no_change_needed", "unsupported"] = "repairable"
    proposed_case: IncidentCase | None = None
    fixture_files: dict[str, str | None] = Field(default_factory=dict)

    @model_validator(mode="after")
    def case_for_repair(self) -> "FixItem":
        if (self.decision == "repairable") != (self.proposed_case is not None):
            raise ValueError("Only repairable fixes require a proposed case")
        return self


class DiagnosisResult(StrictModel):
    decision: Literal[
        "repairable", "no_change_needed", "already_resolved", "unsupported"
    ]
    reasons: tuple[str, ...] = Field(min_length=1)
    fix_items: tuple[FixItem, ...]

    @model_validator(mode="after")
    def valid_items(self) -> "DiagnosisResult":
        if (self.decision == "repairable") != bool(self.fix_items):
            raise ValueError(
                "repairable diagnosis requires nonempty fix_items; "
                "non-repairable diagnosis requires fix_items=[]"
            )
        return self


class EvalReview(StrictModel):
    decision: Literal["approve", "reject"]
    reasons: tuple[str, ...] = Field(min_length=1)
    evidence_ids: tuple[UUID, ...]
    replay_assessment: str


class CandidateProposal(StrictModel):
    fix_id: UUID
    hypothesis: str
    mechanism: str
    declared_paths: tuple[str, ...]
    regression_risks: tuple[str, ...]


RoleResult: TypeAlias = DiagnosisResult | EvalReview | CandidateProposal
RESULT_TYPES: dict[str, type[StrictModel]] = {
    "diagnosis": DiagnosisResult,
    "review": EvalReview,
    "proposal": CandidateProposal,
}


class RoleInfrastructureError(RuntimeError):
    pass


class RoleOutputError(ValueError):
    pass


class RoleLimitError(RoleOutputError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"Role reached {code}; use targeted evidence retrieval.")


def role_result_schema(role: Role) -> dict[str, Any]:
    """Proposal wire contract; prepared cases retain their strict stored schema."""
    schema = RESULT_TYPES[role].model_json_schema()
    if role != "diagnosis":
        return schema
    for name in ("ToolCase", "ContextSelectionCase", "CompactionCase"):
        definition = schema["$defs"][name]
        for field in ("fixture_manifest_digest", "settings"):
            definition["properties"].pop(field)
            definition["required"].remove(field)
    schema["anyOf"] = [
        {
            "properties": {
                "decision": {"const": "repairable"},
                "fix_items": {"minItems": 1},
            }
        },
        {
            "properties": {
                "decision": {
                    "enum": ["unsupported", "no_change_needed", "already_resolved"]
                },
                "fix_items": {"maxItems": 0},
            }
        },
    ]
    return schema


def parse_role_result(role: Role, raw: str) -> RoleResult:
    if len(raw.encode()) > 1_000_000:
        raise RoleOutputError("Role result exceeds output limit")
    try:
        value = json.loads(raw)
        if role == "diagnosis" and isinstance(value, dict):
            items = value.get("fix_items")
            for item in items if isinstance(items, list) else []:
                case = item.get("proposed_case") if isinstance(item, dict) else None
                if isinstance(case, dict):
                    # These placeholders are never authoritative: _prepare_case
                    # derives both values from protected historical evidence.
                    case.setdefault("fixture_manifest_digest", "")
                    case.setdefault("settings", {})
        return cast(RoleResult, RESULT_TYPES[role].model_validate(value))
    except ValidationError as error:
        details = []
        schema = RESULT_TYPES[role].model_json_schema()
        fields = set(schema.get("properties", {}))
        for definition in schema.get("$defs", {}).values():
            fields.update(definition.get("properties", {}))
        for issue in error.errors(include_input=False, include_url=False)[:5]:
            location = (
                ".".join(
                    str(part) if isinstance(part, int) or part in fields else "[key]"
                    for part in issue["loc"]
                )[:180]
                or "$"
            )
            # Never echo model-controlled values, custom validator messages,
            # or Pydantic context (which can contain historical input).
            details.append(f"{location}: {issue['type']}")
        guidance = (
            " A repairable diagnosis requires nonempty fix_items; "
            "a non-repairable diagnosis requires fix_items=[]."
            if role == "diagnosis"
            else ""
        )
        raise RoleOutputError(
            "Invalid role result: " + "; ".join(details) + guidance
        ) from None
    except ValueError:
        raise RoleOutputError("Role returned an invalid structured result") from None


class EvidenceArguments(ToolArguments):
    evidence_id: UUID


class MessageArguments(ToolArguments):
    start: int = Field(default=0, ge=0)
    limit: int = Field(default=10, ge=1, le=50)


class DetailArguments(ToolArguments):
    kind: Literal["message", "message_part", "tool_call", "tool_result", "evidence"]
    identity: UUID
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=4000, ge=1, le=4000)


class IncidentDetailTool:
    parallel_safe = True
    argument_model: type[ToolArguments] = DetailArguments

    def __init__(self, incident: IncidentContext, repair_id: UUID) -> None:
        self.incident, self.repair_id = incident, repair_id
        self.definition = ModelToolDefinition(
            "read_incident_detail",
            "Read one source message, part, tool call/result or evidence record by ID. "
            "Character offsets; at most 4000 characters per page. "
            "Fetch only relevant details.",
            self.argument_model.model_json_schema(),
        )

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
        args = DetailArguments.model_validate(arguments)
        value = await self.incident.read_detail(
            self.repair_id, args.kind, args.identity, args.offset, args.limit
        )
        return ToolOutcome(value, json.dumps(value, ensure_ascii=False))


class IncidentTool:
    parallel_safe = True

    def __init__(
        self, incident: IncidentContext, repair_id: UUID, messages: bool
    ) -> None:
        self.incident, self.repair_id, self.messages = incident, repair_id, messages
        self.argument_model: type[ToolArguments] = (
            MessageArguments if messages else EvidenceArguments
        )
        self.definition = ModelToolDefinition(
            "read_incident_messages" if messages else "read_incident_evidence",
            "Read bounded evidence from this repair's source chat only.",
            self.argument_model.model_json_schema(),
        )

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
        if self.messages:
            args = MessageArguments.model_validate(arguments)
            value = await self.incident.read_messages(
                self.repair_id, args.start, args.limit
            )
        else:
            evidence = EvidenceArguments.model_validate(arguments)
            value = await self.incident.read_evidence(
                self.repair_id, evidence.evidence_id
            )
            encoded = canonical_json(value).decode()
            if len(encoded.encode()) > 8000:
                value = {
                    "evidence_id": str(evidence.evidence_id),
                    "kind": value.get("kind"),
                    "complete": value.get("complete"),
                    "source_ids": value.get("source_ids"),
                    "preview": encoded[:1000],
                    "truncated": True,
                    "detail_tool": "read_incident_detail(kind=evidence)",
                }
        return ToolOutcome(value, json.dumps(value, default=str, ensure_ascii=False))


class SourceTool:
    """Source browsing excludes private/test/eval state before reading any bytes."""

    parallel_safe = True

    def __init__(self, tool: Tool) -> None:
        self.tool, self.definition, self.argument_model = (
            tool,
            tool.definition,
            tool.argument_model,
        )

    @staticmethod
    def allowed(path: Path) -> bool:
        return not any(
            p.casefold()
            in {
                ".git",
                ".env",
                "evals",
                "tests",
                "data",
                ".vibe-wise",
                ".agents",
                ".codex",
                ".venv",
                "node_modules",
                "__pycache__",
            }
            or p.casefold().startswith(".env.")
            for p in path.parts
        )

    @staticmethod
    def _read(root: Path, relative: Path) -> bytes:
        descriptors = [os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)]
        try:
            for part in relative.parts[:-1]:
                descriptors.append(
                    os.open(
                        part,
                        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                        dir_fd=descriptors[-1],
                    )
                )
            descriptor = os.open(
                relative.name,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                dir_fd=descriptors[-1],
            )
            import stat

            with os.fdopen(descriptor, "rb") as source:
                if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
                    raise ValueError("Source is not a regular file")
                return source.read(100001)
        finally:
            for descriptor in reversed(descriptors):
                os.close(descriptor)

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
        from crucible.tools.workspace_authority import WorkspacePathResolver

        values = self.argument_model.model_validate(arguments).model_dump()
        requested = Path(str(values.get("path", ".")))
        if not self.allowed(requested):
            raise ValueError("Source path is unavailable")
        root = WorkspacePathResolver(context.workspace).resolve(str(requested))
        if root.is_symlink() or not self.allowed(
            root.relative_to(context.workspace.resolve())
        ):
            raise ValueError("Source path is unavailable")
        if self.definition.name == "read_file":
            raw = self._read(context.workspace, requested)
            truncated = len(raw) > 100000
            lines = raw[:100000].decode("utf-8").splitlines()
            start = int(values.get("start_line", 1))
            end = values.get("end_line") or len(lines)
            text = "\n".join(
                f"{number}: {line}"
                for number, line in enumerate(lines, 1)
                if start <= number <= int(end)
            )
            return ToolOutcome(
                {"path": str(requested), "content": text}, text, truncated
            )
        paths = []
        for path in context.workspace.rglob("*"):
            relative = path.relative_to(context.workspace)
            if (
                not self.allowed(relative)
                or path.is_symlink()
                or any(
                    p.is_symlink()
                    for p in path.parents
                    if p != context.workspace and p.is_relative_to(context.workspace)
                )
            ):
                continue
            if path.is_file() and path.is_relative_to(root):
                paths.append(path)
            if len(paths) >= 1000:
                break
        if self.definition.name == "list_files":
            names = [str(p.relative_to(context.workspace)) for p in sorted(paths)]
            return ToolOutcome({"paths": names}, "\n".join(names))
        query = str(values["query"])
        matches: list[str] = []
        size = 0
        for path in sorted(paths):
            if path.stat().st_size > 100000:
                continue
            try:
                lines = (
                    self._read(context.workspace, path.relative_to(context.workspace))
                    .decode("utf-8")
                    .splitlines()
                )
            except (UnicodeError, OSError):
                continue
            for number, line in enumerate(lines, 1):
                if query in line:
                    item = f"{path.relative_to(context.workspace)}:{number}:{line}"
                    if size + len(item.encode()) > 80000:
                        return ToolOutcome(
                            {"matches": matches}, "\n".join(matches), True
                        )
                    matches.append(item)
                    size += len(item.encode())
        return ToolOutcome({"matches": matches}, "\n".join(matches))


class EditableTool:
    def __init__(self, tool: Tool) -> None:
        self.tool, self.definition, self.argument_model = (
            tool,
            tool.definition,
            tool.argument_model,
        )
        self.parallel_safe = False

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
        from crucible.tools.execution_policy import admit_mutation

        admission = admit_mutation(
            self.definition.name,
            self.argument_model.model_validate(arguments).model_dump(),
            context.workspace,
        )
        if not admission.paths or not set(admission.paths) <= EDITABLE_PATHS:
            raise ValueError("Proposal targets protected source")
        return await self.tool.invoke(
            replace(context, workspace_authority=admission.authority), arguments
        )


class ProposalTracker:
    """Record a controller-observed digest after every mutation/command boundary."""

    def __init__(
        self,
        factory: Callable[[], UnitOfWork],
        clock: Clock,
        expected_digest: str,
        notifier: EventNotifier | None = None,
    ) -> None:
        self.factory, self.clock, self.expected_digest = factory, clock, expected_digest
        self.notifier = notifier

    async def before(self, context: ToolContext) -> None:
        from crucible.repair.fingerprints import capture_source_identity

        async with self.factory() as uow:
            attempt = (
                await uow.repairs.attempt_for_run(context.run_id)
                if context.run_id
                else None
            )
            repair = await uow.repairs.get(attempt.repair_id) if attempt else None
            owner = await uow.repairs.owner()
        if (
            repair is None
            or repair.status is not RepairStatus.RUNNING
            or owner.repair_id != repair.id
        ):
            raise ValueError("Proposal mutation has no live repair authority")
        if (
            await capture_source_identity(context.workspace)
        ).tree_digest != self.expected_digest:
            raise ValueError("Unexpected edits occurred between proposal tools")

    async def after(self, context: ToolContext) -> None:
        from crucible.domain.repair import RepairCheckpoint
        from crucible.repair.fingerprints import capture_source_identity

        source = await capture_source_identity(context.workspace)
        async with self.factory() as uow:
            attempt = (
                await uow.repairs.attempt_for_run(context.run_id)
                if context.run_id
                else None
            )
            if attempt is None:
                raise ValueError("Mutation has no repair attempt")
            repair = await uow.repairs.get(attempt.repair_id)
            if repair is None:
                raise ValueError("Repair is unavailable")
            await uow.repairs.add_checkpoint(
                RepairCheckpoint(
                    uuid4(),
                    repair.id,
                    repair.stage,
                    attempt.fix_id,
                    None,
                    source.tree_digest,
                    repair.last_accepted_commit,
                    {},
                    {
                        "owned_mutation": {
                            "attempt_id": str(attempt.id),
                            "run_id": str(context.run_id),
                            "tool_call_id": str(context.tool_call_id),
                            "source_digest": source.tree_digest,
                        }
                    },
                    self.clock.now(),
                )
            )
            await append_repair_event(uow, self.clock, repair)
            await uow.commit()
        self.expected_digest = source.tree_digest
        if self.notifier:
            await self.notifier.notify(repair.source_task_id)


class TrackedProposalTool:
    def __init__(self, tool: Tool, tracker: ProposalTracker) -> None:
        self.tool, self.tracker = tool, tracker
        self.definition, self.argument_model, self.parallel_safe = (
            tool.definition,
            tool.argument_model,
            False,
        )

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
        await self.tracker.before(context)
        try:
            return await self.tool.invoke(context, arguments)
        finally:
            # Command cancellation finishes its sandbox cleanup before returning.
            recording = asyncio.create_task(self.tracker.after(context))
            await asyncio.shield(recording)


class SafeRoleTool:
    """Known credentials are removed from every model-visible tool result."""

    def __init__(self, tool: Tool, incident: IncidentContext) -> None:
        self.tool, self.incident = tool, incident
        self.definition = tool.definition
        self.argument_model, self.parallel_safe = (
            tool.argument_model,
            tool.parallel_safe,
        )

    async def invoke(self, context: ToolContext, arguments: object) -> ToolOutcome:
        outcome = await self.tool.invoke(context, arguments)
        return replace(
            outcome,
            data=self.incident.safe(dict(outcome.data)),
            display_text=self.incident.safe(outcome.display_text),
        )


class RoleExecutor:
    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        artifacts: ArtifactService,
        engine_factory: Callable[[ToolRegistry], RunEngine],
        incident: IncidentContext,
        *,
        command_tool: Tool | None = None,
        resources: TaskResourceManager | None = None,
        notifier: EventNotifier | None = None,
        configuration: dict[str, Any] | None = None,
    ) -> None:
        self.factory, self.clock, self.artifacts, self.engine_factory, self.incident = (
            unit_of_work,
            clock,
            artifacts,
            engine_factory,
            incident,
        )
        self.command_tool = command_tool
        self.resources = resources
        self.notifier = notifier
        self.configuration = configuration or {}
        self.active: dict[UUID, asyncio.Task[bool]] = {}

    @property
    def configuration_fingerprint(self) -> str:
        return digest(self.configuration)

    def registry(
        self, repair_id: UUID, role: Role, tracker: ProposalTracker | None = None
    ) -> ToolRegistry:
        from crucible.tools.filesystem import (
            ListFilesTool,
            ReadFileTool,
            SearchFilesTool,
        )
        from crucible.tools.git_tools import (
            ApplyPatchTool,
            WorkspaceDiffTool,
            WorkspaceStatusTool,
            WriteFileTool,
        )

        tools: list[Tool] = [
            SourceTool(cast(Tool, t))
            for t in (ListFilesTool(), ReadFileTool(), SearchFilesTool())
        ]
        tools += [
            IncidentTool(self.incident, repair_id, False),
            IncidentTool(self.incident, repair_id, True),
            IncidentDetailTool(self.incident, repair_id),
        ]
        if role == "proposal":
            tools += [
                EditableTool(cast(Tool, WriteFileTool())),
                EditableTool(cast(Tool, ApplyPatchTool())),
                cast(Tool, WorkspaceStatusTool()),
                cast(Tool, WorkspaceDiffTool()),
            ]
            if self.command_tool is not None:
                tools.append(self.command_tool)
        if tracker is not None:
            tools = [
                TrackedProposalTool(tool, tracker)
                if tool.definition.name
                in {"write_file", "apply_patch", "execute_command"}
                else tool
                for tool in tools
            ]
        return ToolRegistry(tuple(SafeRoleTool(tool, self.incident) for tool in tools))

    async def execute(
        self,
        repair_id: UUID,
        role: Role,
        fix_id: UUID | None,
        attempt_id: UUID,
        input_artifact_ids: tuple[UUID, ...],
    ) -> RoleResult:
        index = await self.incident.index(await self.incident._source(repair_id))
        async with self.factory() as uow:
            repair = await uow.repairs.get(repair_id)
            owner = await uow.repairs.owner()
            source = await uow.tasks.get(index.source_task_id)
            attempts = await uow.repairs.list_attempts(repair_id)
            attempt = next((a for a in attempts if a.id == attempt_id), None)
            if (
                repair is None
                or repair.status is not RepairStatus.RUNNING
                or owner.repair_id != repair_id
                or source is None
                or attempt is None
                or attempt.role != role
                or attempt.fix_id != fix_id
                or attempt.harness_run_id is not None
            ):
                raise ValueError("Role execution has no live controller authority")
            if repair.worktree_path is None:
                raise RoleInfrastructureError("Repair worktree is unavailable")
            repository = await uow.repositories.get(source.repository_id)
            if repository is None:
                raise RoleInfrastructureError("Source repository is unavailable")
        if role == "proposal":
            from crucible.repair.recovery import ManagedRepairWorkspace

            await ManagedRepairWorkspace(self.factory, self.artifacts).prepare(repair)
        inputs = []
        for artifact_id in input_artifact_ids:
            metadata, raw = await self.artifacts.read(artifact_id)
            # The controller passes only its own visible role/checkpoint records.
            if metadata.task_id != source.id or metadata.sensitivity != "private":
                raise ValueError("Foreign role input")
            inputs.append(json.loads(raw))
        prompt = role_prompt(
            role,
            role_result_schema(role),
            asdict(index),
            self.incident.safe(inputs),
        )
        settings = replace(repository.settings, validation_commands=())
        task = replace(
            Task.provisioning(
                task_id=uuid4(),
                repository_id=source.repository_id,
                source_ref=repair.branch or "HEAD",
                base_revision=repair.last_accepted_commit,
                workspace_path=Path(repair.worktree_path),
                clock=self.clock,
            ),
            status=TaskStatus.ACTIVE,
            internal_repair_id=repair_id,
        )
        run = Run.queued(
            run_id=uuid4(),
            task_id=task.id,
            triggering_message_id=None,
            created_at=self.clock.now(),
            settings_snapshot=settings,
        )
        message = Message(
            uuid4(),
            task.id,
            run.id,
            None,
            0,
            MessageRole.USER,
            MessageStatus.COMPLETED,
            (MessagePart(uuid4(), 1, MessagePartKind.TEXT, prompt),),
            self.clock.now(),
            self.clock.now(),
        )
        async with self.factory() as uow:
            await uow.repairs.lock()
            current = await uow.repairs.get(repair_id)
            if (
                current is None
                or current.status is not RepairStatus.RUNNING
                or (await uow.repairs.owner()).repair_id != repair_id
            ):
                raise RoleInfrastructureError("Repair stopped before role dispatch")
            await uow.tasks.add(task)
            await uow.runs.add(run)
            saved = await uow.messages.add(message)
            await uow.runs.set_triggering_message(run.id, saved.id)
            await uow.repairs.update_attempt(
                replace(
                    attempt,
                    harness_run_id=run.id,
                    charged=True,
                    status="running",
                    started_at=self.clock.now(),
                )
            )
            await append_repair_event(uow, self.clock, current)
            await uow.commit()
        if self.notifier:
            await self.notifier.notify(repair.source_task_id)
        if role == "proposal" and self.resources is not None:
            try:
                await self.resources.ensure_dependency_volume(task.id)
            except Exception:
                raise RoleInfrastructureError(
                    "Role command resources are unavailable"
                ) from None
        tracker = None
        if role == "proposal":
            from crucible.repair.fingerprints import capture_source_identity

            tracker = ProposalTracker(
                self.factory,
                self.clock,
                (await capture_source_identity(task.workspace_path)).tree_digest,
                self.notifier,
            )
        engine = self.engine_factory(self.registry(repair_id, role, tracker))
        execution = asyncio.create_task(engine.execute(run.id))
        self.active[repair_id] = execution
        try:
            await execution
        finally:
            self.active.pop(repair_id, None)
        async with self.factory() as uow:
            terminal = await uow.runs.get(run.id)
            messages = await uow.messages.list_for_task(task.id)
        if terminal is None or terminal.status is not RunStatus.COMPLETED:
            if terminal and terminal.outcome_code in {
                "budget_exhausted",
                "context_limit",
            }:
                raise RoleLimitError(terminal.outcome_code)
            raise RoleInfrastructureError("Role harness execution did not complete")
        final = next(
            (m for m in reversed(messages) if m.role is MessageRole.ASSISTANT), None
        )
        final_text = (
            "".join(
                p.text_content or ""
                for p in final.parts
                if p.kind is MessagePartKind.TEXT
            )
            if final
            else ""
        )
        return parse_role_result(role, final_text)

    async def stop(self, repair_id: UUID) -> None:
        task = self.active.get(repair_id)
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

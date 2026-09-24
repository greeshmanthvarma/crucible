from collections.abc import AsyncIterator, Callable
from uuid import UUID

from crucible.application.artifact_service import ArtifactService
from crucible.application.errors import AcceptanceRejected, TaskNotFound
from crucible.application.idempotency import IdempotencyRecord, canonical_request_hash
from crucible.application.ports import EventNotifier, UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.events import EventFactory, EventType
from crucible.domain.ids import new_id
from crucible.domain.results import ResultRevision
from crucible.domain.run import RunStatus
from crucible.domain.validation import ValidationStatus
from crucible.workspaces.git import GitClient


async def _bytes(value: bytes) -> AsyncIterator[bytes]:
    yield value


class AcceptanceService:
    def __init__(
        self,
        git: GitClient,
        artifacts: ArtifactService,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        notifier: EventNotifier | None = None,
    ) -> None:
        self._git = git
        self._artifacts = artifacts
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._notifier = notifier
        self._events = EventFactory()

    async def accept(self, task_id: UUID, idempotency_key: str) -> ResultRevision:
        scope = f"POST:/api/tasks/{task_id}/acceptances"
        request_hash = canonical_request_hash({})
        async with self._unit_of_work() as uow:
            previous = await uow.idempotency.get(scope, idempotency_key)
            if previous is not None:
                result = await uow.result_revisions.get(
                    UUID(str(previous.response_json["id"]))
                )
                if result is None:
                    raise RuntimeError(
                        "Acceptance idempotency record lost its Result Revision"
                    )
                return result
            task = await uow.tasks.get(task_id)
            if task is None:
                raise TaskNotFound(f"Task not found: {task_id}")
            active_run = await uow.runs.get_nonterminal_for_task(task_id)
            runs = await uow.runs.list_for_task(task_id)
            revisions = await uow.result_revisions.list_for_task(task_id)
        if active_run is not None:
            raise AcceptanceRejected("Cannot accept while a Run is active")
        if not runs:
            raise AcceptanceRejected("Task has no completed Run")
        run = runs[-1]
        if run.status is not RunStatus.COMPLETED:
            raise AcceptanceRejected("Latest Run did not complete successfully")
        async with self._unit_of_work() as uow:
            attempts = await uow.validation_attempts.list_for_run(run.id)
            proposal = await uow.completion_proposals.get_for_run(run.id)
        if not attempts or proposal is None:
            raise AcceptanceRejected("Latest Run has no Validation evidence")
        validation = attempts[-1]
        required = bool(run.settings_snapshot.validation_commands)
        expected = (
            ValidationStatus.PASSED if required else ValidationStatus.NOT_CONFIGURED
        )
        if validation.status is not expected:
            raise AcceptanceRejected(
                f"Latest Validation status is {validation.status.value}; "
                f"expected {expected.value}"
            )
        owned_commit = await self._git.find_owned_acceptance_commit(
            task.workspace_path, str(task.id), idempotency_key
        )
        if owned_commit is None:
            evidence = await self._git.acceptance_evidence(task.workspace_path)
            if not evidence.changed_files:
                raise AcceptanceRejected("Task Workspace has no changes to accept")
        else:
            evidence = await self._git.commit_evidence(
                task.workspace_path, owned_commit.commit_sha
            )
        artifact = await self._artifacts.put(
            task.id,
            "application/vnd.git-diff",
            "private",
            _bytes(evidence.diff),
        )
        commit = owned_commit or await self._git.create_acceptance_commit(
            task.workspace_path, str(task.id), idempotency_key, self._clock.now()
        )
        if await self._git.worktree_revision(task.workspace_path) != commit.commit_sha:
            raise RuntimeError("Acceptance commit could not be verified")
        verified = await self._git.find_owned_acceptance_commit(
            task.workspace_path, str(task.id), idempotency_key
        )
        if verified != commit:
            raise RuntimeError("Acceptance commit ownership metadata is invalid")
        async with self._unit_of_work() as uow:
            command_results = await uow.validation_command_results.list_for_attempt(
                validation.id
            )
        snapshot = {
            "attemptId": str(validation.id),
            "status": validation.status.value,
            "commands": [
                {
                    "sequence": item.command_sequence,
                    "status": item.status.value,
                    "approvalId": str(item.approval_id) if item.approval_id else None,
                    "toolCallId": str(item.tool_call_id) if item.tool_call_id else None,
                    "artifactId": str(item.artifact_id) if item.artifact_id else None,
                    "exitCode": item.exit_code,
                }
                for item in command_results
            ],
            "changedFiles": list(evidence.changed_files),
        }
        result = ResultRevision(
            new_id(),
            task.id,
            commit.commit_sha,
            commit.parent_revision,
            revisions[-1].id if revisions else None,
            artifact.id,
            snapshot,
            proposal.summary,
            "local_user",
            self._clock.now(),
        )
        async with self._unit_of_work() as uow:
            await uow.result_revisions.add(result)
            await uow.tasks.update(task.accept(self._clock))
            await uow.events.append(
                self._events.create(
                    task_id=task.id,
                    run_id=None,
                    type=EventType.TASK_ACCEPTED,
                    payload={"result_revision_id": str(result.id)},
                    created_at=self._clock.now(),
                )
            )
            await uow.idempotency.add(
                IdempotencyRecord(
                    new_id(),
                    scope,
                    idempotency_key,
                    request_hash,
                    201,
                    {"id": str(result.id)},
                    self._clock.now(),
                )
            )
            await uow.commit()
        if self._notifier is not None:
            await self._notifier.notify(task.id)
        return result

from collections.abc import Callable
from dataclasses import dataclass
from uuid import UUID

from crucible.application.errors import (
    ApprovalConflict,
    ApprovalNotFound,
    IdempotencyConflict,
    TaskNotFound,
)
from crucible.application.idempotency import IdempotencyRecord, canonical_request_hash
from crucible.application.ports import EventNotifier, UnitOfWork
from crucible.domain.approvals import Approval, ApprovalStatus
from crucible.domain.clock import Clock
from crucible.domain.events import Event, EventFactory, EventType
from crucible.domain.ids import new_id
from crucible.domain.run import RunStatus
from crucible.engine.approval_broker import InMemoryApprovalBroker


@dataclass(frozen=True)
class ApprovalDecisionResult:
    approval: Approval
    response_status: int = 200

    def response_json(self) -> dict[str, object]:
        return approval_response(self.approval)


class ApprovalService:
    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        broker: InMemoryApprovalBroker,
        notifier: EventNotifier | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._broker = broker
        self._notifier = notifier
        self._events = EventFactory()

    async def request(self, approval: Approval) -> Approval:
        async with self._unit_of_work() as uow:
            await uow.approvals.add(approval)
            await uow.events.append(self._event(approval, EventType.APPROVAL_REQUESTED))
            await uow.commit()
        await self._notify(approval.task_id)
        return approval

    async def get(self, approval_id: UUID) -> Approval:
        async with self._unit_of_work() as uow:
            approval = await uow.approvals.get(approval_id)
        if approval is None:
            raise ApprovalNotFound(f"Approval not found: {approval_id}")
        return approval

    async def list_for_task(self, task_id: UUID) -> tuple[Approval, ...]:
        async with self._unit_of_work() as uow:
            if await uow.tasks.get(task_id) is None:
                raise TaskNotFound(f"Task not found: {task_id}")
            return await uow.approvals.list_for_task(task_id)

    async def decide(
        self,
        approval_id: UUID,
        decision: ApprovalStatus,
        spec_digest: str,
        reason: str | None,
        idempotency_key: str,
        *,
        decided_by: str = "local-user",
    ) -> ApprovalDecisionResult:
        if decision not in (ApprovalStatus.APPROVED, ApprovalStatus.DENIED):
            raise ApprovalConflict("Decision must be approved or denied")
        scope = f"approval:{approval_id}:decision"
        request_hash = canonical_request_hash(
            {
                "decision": decision.value,
                "reason": reason,
                "spec_digest": spec_digest,
            }
        )
        async with self._unit_of_work() as uow:
            previous = await uow.idempotency.get(scope, idempotency_key)
            if previous is not None:
                if previous.request_hash != request_hash:
                    raise IdempotencyConflict(
                        "Idempotency key was already used for a different request"
                    )
                replay = await uow.approvals.get(approval_id)
                if replay is None:
                    raise RuntimeError(
                        "Idempotency record references a missing Approval"
                    )
                return ApprovalDecisionResult(replay, previous.response_status)

            approval = await uow.approvals.get(approval_id)
            if approval is None:
                raise ApprovalNotFound(f"Approval not found: {approval_id}")
            if approval.spec_digest != spec_digest:
                raise ApprovalConflict("Command specification digest has changed")
            if approval.status is not ApprovalStatus.PENDING:
                raise ApprovalConflict("Approval has already been decided")
            run = await uow.runs.get(approval.run_id)
            if run is None or run.status not in (RunStatus.QUEUED, RunStatus.RUNNING):
                raise ApprovalConflict("Approval Run is no longer active")
            unresolved = await uow.tool_calls.list_without_result_for_run(run.id)
            if not any(call.id == approval.tool_call_id for call in unresolved):
                raise ApprovalConflict("Approval Tool Call is already resolved")

            now = self._clock.now()
            decided = (
                approval.approve(decided_by, now)
                if decision is ApprovalStatus.APPROVED
                else approval.deny(decided_by, reason, now)
            )
            if not await uow.approvals.decide_pending(decided):
                raise ApprovalConflict("Approval was decided concurrently")
            event_type = (
                EventType.APPROVAL_APPROVED
                if decision is ApprovalStatus.APPROVED
                else EventType.APPROVAL_DENIED
            )
            await uow.events.append(self._event(decided, event_type))
            result = ApprovalDecisionResult(decided)
            await uow.idempotency.add(
                IdempotencyRecord(
                    new_id(),
                    scope,
                    idempotency_key,
                    request_hash,
                    result.response_status,
                    result.response_json(),
                    now,
                )
            )
            await uow.commit()

        await self._notify(decided.task_id)
        await self._broker.publish(decided.id, decided.status)
        return result

    def _event(self, approval: Approval, event_type: EventType) -> Event:
        return self._events.create(
            task_id=approval.task_id,
            run_id=approval.run_id,
            type=event_type,
            created_at=self._clock.now(),
            payload={
                "approval_id": str(approval.id),
                "tool_call_id": str(approval.tool_call_id),
                "spec_digest": approval.spec_digest,
                "status": approval.status.value,
            },
        )

    async def _notify(self, task_id: UUID) -> None:
        if self._notifier is not None:
            await self._notifier.notify(task_id)


def approval_response(approval: Approval) -> dict[str, object]:
    return {
        "id": str(approval.id),
        "taskId": str(approval.task_id),
        "runId": str(approval.run_id),
        "stepId": str(approval.step_id),
        "toolCallId": str(approval.tool_call_id),
        "spec": approval.spec.as_dict(),
        "specDigest": approval.spec_digest,
        "status": approval.status.value,
        "decisionReason": approval.decision_reason,
        "decidedBy": approval.decided_by,
        "createdAt": approval.created_at.isoformat(),
        "decidedAt": (
            approval.decided_at.isoformat() if approval.decided_at is not None else None
        ),
    }

"""One safe durable event shape for repair state and role counter changes."""

from crucible.application.ports import UnitOfWork
from crucible.domain.clock import Clock
from crucible.domain.events import EventFactory, EventType
from crucible.domain.repair import RepairRun


async def append_repair_event(uow: UnitOfWork, clock: Clock, repair: RepairRun) -> None:
    attempts = await uow.repairs.list_attempts(repair.id)
    publication = await uow.repairs.get_publication(repair.id)
    await uow.events.append(
        EventFactory().create(
            task_id=repair.source_task_id,
            run_id=None,
            type=EventType.REPAIR_UPDATED,
            created_at=clock.now(),
            payload={
                "source_task_id": str(repair.source_task_id),
                "repair_id": str(repair.id),
                "status": repair.status,
                "stage": repair.stage,
                "attempts": {
                    role: sum(a.charged for a in attempts if a.role == role)
                    for role in {a.role for a in attempts}
                },
                "outcome_code": repair.outcome_code,
                "detail": repair.outcome_detail,
                "pr_url": publication.pr_url if publication else None,
            },
        )
    )

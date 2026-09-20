from uuid import UUID

type RepositoryId = UUID
type TaskId = UUID
type RunId = UUID
type MessageId = UUID
type MessagePartId = UUID
type EventId = UUID
type ExecutionId = UUID


def new_id() -> UUID:
    from uuid6 import uuid7

    return uuid7()

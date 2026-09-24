from uuid import UUID

type RepositoryId = UUID
type TaskId = UUID
type RunId = UUID
type StepId = UUID
type MessageId = UUID
type MessagePartId = UUID
type ToolCallId = UUID
type ToolResultId = UUID
type ContextManifestId = UUID
type EventId = UUID
type ExecutionId = UUID
type ApprovalId = UUID
type ArtifactId = UUID
type ExternalResourceId = UUID
type CompactionId = UUID
type ValidationAttemptId = UUID
type ValidationCommandResultId = UUID
type CompletionProposalId = UUID
type ResultRevisionId = UUID
type IntegrationId = UUID
type AuthSessionId = UUID


def new_id() -> UUID:
    from uuid6 import uuid7

    return uuid7()

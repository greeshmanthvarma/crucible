class ApplicationError(Exception):
    code = "application_error"
    status_code = 422

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class RepositoryPathNotFound(ApplicationError):
    code = "repository_path_not_found"


class NotAGitRepository(ApplicationError):
    code = "not_a_git_repository"


class BareRepositoryUnsupported(ApplicationError):
    code = "bare_repository_unsupported"


class RepositoryHasNoCommit(ApplicationError):
    code = "repository_has_no_commit"


class RevisionNotFound(ApplicationError):
    code = "revision_not_found"


class WorkspaceDestinationExists(ApplicationError):
    code = "workspace_destination_exists"


class WorkspaceRevisionMismatch(ApplicationError):
    code = "workspace_revision_mismatch"


class RepositoryNotFound(ApplicationError):
    code = "repository_not_found"
    status_code = 404


class TaskNotFound(ApplicationError):
    code = "task_not_found"
    status_code = 404


class WorkspaceProvisioningFailed(ApplicationError):
    code = "workspace_provisioning_failed"


class IdempotencyConflict(ApplicationError):
    code = "idempotency_conflict"
    status_code = 409


class IdempotencyKeyRequired(ApplicationError):
    code = "idempotency_key_required"
    status_code = 400


class ApprovalNotFound(ApplicationError):
    code = "approval_not_found"
    status_code = 404


class ApprovalConflict(ApplicationError):
    code = "approval_conflict"
    status_code = 409


class ArtifactNotFound(ApplicationError):
    code = "artifact_not_found"
    status_code = 404


class TaskNotActive(ApplicationError):
    code = "task_not_active"
    status_code = 409


class EventCursorNotFound(ApplicationError):
    code = "event_cursor_not_found"
    status_code = 409


class EventCursorTaskMismatch(ApplicationError):
    code = "event_cursor_task_mismatch"
    status_code = 409

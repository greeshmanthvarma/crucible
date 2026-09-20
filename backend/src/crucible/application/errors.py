class ApplicationError(Exception):
    code = "application_error"

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

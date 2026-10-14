"""Editable descriptions and compatible, non-authority argument translation."""

from collections.abc import Mapping

DESCRIPTIONS = {
    "list_files": "List stable workspace-relative file paths.",
    "search_files": "Search workspace text with ripgrep.",
    "read_file": "Read a bounded UTF-8 workspace file range.",
    "write_file": "Atomically write complete UTF-8 content inside the workspace.",
    "apply_patch": "Apply a standard unified diff inside the workspace. "
    "Include --- a/path, "
    "+++ b/path, and numbered @@ -old +new @@ hunk headers. "
    "The *** Begin Patch / *** Update File format is not supported.",
    "workspace_status": "Inspect Git workspace status.",
    "workspace_diff": "Inspect the current Git workspace diff.",
    "execute_command": "Request approval, then execute one structured command "
    "in Docker. "
    "The harness selects the pinned sandbox image. Use cwd (or workdir) "
    "for a path relative to the workspace.",
}


def adapt_arguments(name: str, arguments: Mapping[str, object]) -> dict[str, object]:
    return dict(arguments)

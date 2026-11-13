"""Editable descriptions and compatible, non-authority argument translation."""

from collections.abc import Mapping

DESCRIPTIONS = {
    "list_files": "List workspace-relative paths breadth-first; "
    "directories end with /. "
    "Large trees are truncated; pass a subdirectory path to see deeper levels.",
    "search_files": "Search workspace text with a ripgrep regular expression. "
    "Returns bounded matching excerpts with workspace-relative paths, line numbers "
    "and UTF-8 byte offsets, including on long lines. Pass next_cursor unchanged "
    "with the same query and path to continue when truncated. No matches is "
    "conclusive only when truncated is false and there is no error. "
    "A snippet may omit part of a large match; use its offsets for bounded reads. "
    "Search is not a snapshot: restart without a cursor if files change.",
    "read_file": "Read a bounded UTF-8 workspace file range.",
    "write_file": "Atomically write complete UTF-8 content inside the workspace.",
    "edit_file": "Replace exact text in one workspace file. Copy old_text exactly "
    "from read_file output, including indentation, with enough surrounding lines "
    "to match once; set replace_all to change every match. Prefer this for "
    "targeted edits; use write_file to create files.",
    "apply_patch": "Prefer edit_file for targeted changes. "
    "Apply a standard unified diff inside the workspace. "
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

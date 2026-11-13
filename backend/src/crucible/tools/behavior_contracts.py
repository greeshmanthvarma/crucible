"""Editable descriptions and compatible, non-authority argument translation."""

import re
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
    adapted = dict(arguments)
    if name == "apply_patch" and isinstance(adapted.get("patch"), str):
        adapted["patch"] = normalize_unified_diff(str(adapted["patch"]))
    return adapted


def normalize_unified_diff(patch: str) -> str:
    lines: list[str] = []
    for line in patch.splitlines():
        if line.rstrip() in {"*** Begin Patch", "*** End Patch"}:
            continue
        if line.startswith("*** Update File: "):
            path = line[len("*** Update File: ") :]
            lines.extend((f"--- a/{path}", f"+++ b/{path}"))
        else:
            lines.append(line)

    def file_header(index: int) -> bool:
        return (
            lines[index].startswith("--- ")
            and index + 1 < len(lines)
            and lines[index + 1].startswith("+++ ")
        )

    output: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        match = re.fullmatch(r"@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@(.*)", line)
        if match is None:
            output.append(line)
            index += 1
            continue
        end = index + 1
        while end < len(lines):
            if lines[end].startswith(("@@ ", "diff --git ")) or file_header(end):
                break
            end += 1
        body = lines[index + 1 : end]
        while body and not body[-1]:
            body.pop()
        body = [entry or " " for entry in body]
        if any(entry[0] not in " +-\\" for entry in body):
            return patch
        old = sum(entry[0] in " -" for entry in body)
        new = sum(entry[0] in " +" for entry in body)
        output.append(f"@@ -{match[1]},{old} +{match[2]},{new} @@{match[3]}")
        output.extend(body)
        index = end
    return "\n".join(output) + "\n"

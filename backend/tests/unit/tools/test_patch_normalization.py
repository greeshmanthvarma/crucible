from crucible.tools.behavior_contracts import normalize_unified_diff
from crucible.tools.workspace_authority import patch_paths


def test_recount_fixes_wrong_hunk_counts() -> None:
    patch = "--- a/x\n+++ b/x\n@@ -1,99 +1,42 @@ tail\n-old\n+new\n keep\n"
    normalized = normalize_unified_diff(patch)
    assert normalized == patch.replace("-1,99 +1,42", "-1,2 +1,2")
    assert patch_paths(normalized) == ("x",)


def test_strips_wrapper_lines_and_converts_update_file_header() -> None:
    patch = (
        "*** Begin Patch\n*** Update File: x\n@@ -1 +1 @@\n-old\n+new\n*** End Patch"
    )
    assert (
        normalize_unified_diff(patch)
        == "--- a/x\n+++ b/x\n@@ -1,1 +1,1 @@\n-old\n+new\n"
    )


def test_recount_keeps_removed_double_dash_line() -> None:
    patch = "--- a/x.sql\n+++ b/x.sql\n@@ -1,1 +1,1 @@\n--- old comment\n keep\n"
    assert normalize_unified_diff(patch) == patch.replace("-1,1 +1,1", "-1,2 +1,1")


def test_unknown_body_line_returns_original() -> None:
    patch = "*** Begin Patch\n*** Update File: x\n@@ -1 +1 @@\nunknown\n*** End Patch"
    assert normalize_unified_diff(patch) == patch


def test_blank_context_and_multiple_files_and_hunks() -> None:
    patch = (
        "--- a/x\n+++ b/x\n@@ -1,9 +1,9 @@\n-a\n+b\n\n c\n\n"
        "@@ -9 +9 @@\n-d\n+e\n\ndiff --git a/y b/y\n"
        "--- a/y\n+++ b/y\n@@ -1 +1 @@\n-f\n+g\n"
    )
    normalized = normalize_unified_diff(patch)
    assert "@@ -1,3 +1,3 @@\n-a\n+b\n \n c\n@@ -9,1 +9,1 @@" in normalized
    assert patch_paths(normalized) == ("x", "y")


def test_bare_hunk_remains_rejected() -> None:
    import pytest

    patch = "*** Update File: x\n@@\n-a\n+b\n"
    with pytest.raises(ValueError):
        patch_paths(normalize_unified_diff(patch))

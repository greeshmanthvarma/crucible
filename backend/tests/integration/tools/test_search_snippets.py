import asyncio
import json
import sys
from pathlib import Path

import pytest

from crucible.improvement.role_worker import role_registry
from crucible.tools.definitions import SearchFilesArguments, ToolContext
from crucible.tools.filesystem import SearchFilesTool
from crucible.tools.search_io import search_workspace


async def test_role_search_finds_match_at_end_of_large_unicode_line(tmp_path: Path):
    text = "🙂" * 40_000 + " needle café " + "x" * 20_000
    (tmp_path / "trace.json").write_text(text)
    tool = role_registry("mining").get("search_files")
    assert tool is not None
    outcome = await tool.invoke(ToolContext(tmp_path), {"query": "needle café"})
    assert outcome.error_code is None
    assert outcome.data["matches"]
    location = outcome.data["locations"][0]
    assert location["path"] == "trace.json"
    assert (
        text.encode()[location["match_start"] : location["match_end"]].decode()
        == "needle café"
    )
    assert (
        len(json.dumps(outcome.data, ensure_ascii=False).encode())
        + len(outcome.display_text.encode())
        <= 8192
    )


async def test_search_pages_across_files_without_dropping_matches(tmp_path: Path):
    for name in ("a.txt", "b.txt"):
        (tmp_path / name).write_text(("🙂 needle café " + "x" * 100 + "\n") * 8)
    tool = SearchFilesTool(max_matches=3, max_bytes=4096)
    arguments = {"query": "needle café"}
    found = []
    for _ in range(10):
        outcome = await tool.invoke(ToolContext(tmp_path), arguments)
        assert outcome.error_code is None
        found.extend((r["path"], r["match_start"]) for r in outcome.data["locations"])
        if not outcome.truncated:
            assert outcome.data["next_cursor"] is None
            break
        assert outcome.data["next_cursor"] != arguments.get("cursor")
        arguments = {"query": "needle café", "cursor": outcome.data["next_cursor"]}
    else:
        pytest.fail("pagination did not finish")
    assert len(found) == len(set(found)) == 16


async def test_search_distinguishes_absence_and_rejects_wrong_query_cursor(
    tmp_path: Path,
):
    (tmp_path / "a.txt").write_text("needle\nneedle\n")
    tool = SearchFilesTool(max_matches=1)
    missing = await tool.invoke(ToolContext(tmp_path), {"query": "absent"})
    assert missing.data["matches"] == []
    assert not missing.truncated
    assert missing.data["next_cursor"] is None
    first = await tool.invoke(ToolContext(tmp_path), {"query": "needle"})
    changed = await tool.invoke(
        ToolContext(tmp_path), {"query": "other", "cursor": first.data["next_cursor"]}
    )
    assert changed.error_code == "invalid_cursor"


@pytest.mark.parametrize(
    "query,expected", [("(", "search_failed"), (".+", "search_output_limit")]
)
async def test_invalid_regex_and_oversized_match_are_explicit_errors(
    tmp_path, query, expected
):
    (tmp_path / "huge.txt").write_text("x" * 1_000_000)
    outcome = await asyncio.wait_for(
        SearchFilesTool().invoke(ToolContext(tmp_path), {"query": query}), 3
    )
    assert outcome.error_code == expected
    assert outcome.truncated


async def test_scan_and_response_budgets_are_independently_enforced(tmp_path):
    (tmp_path / "a.txt").write_text("needle " * 100)
    args = SearchFilesArguments(query="needle")
    scan = await search_workspace(
        tmp_path, args, max_matches=10, max_bytes=8192, max_scan_bytes=10
    )
    response = await search_workspace(tmp_path, args, max_matches=10, max_bytes=128)
    for outcome in (scan, response):
        assert outcome.error_code == "search_output_limit"
        assert outcome.truncated
    assert (
        len(response.display_text.encode()) + len(json.dumps(response.data).encode())
        <= 128
    )


@pytest.mark.parametrize("cancel", [False, True])
async def test_timeout_and_cancellation_reap_the_subprocess(
    tmp_path, monkeypatch, cancel
):
    original = asyncio.create_subprocess_exec
    processes = []

    async def spawn(*args, **kwargs):
        process = await original(
            sys.executable,
            "-c",
            'import sys,time; sys.stderr.write("x"*200000); '
            "sys.stderr.flush(); time.sleep(60)",
            **kwargs,
        )
        processes.append(process)
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    task = asyncio.create_task(
        search_workspace(
            tmp_path,
            SearchFilesArguments(query="needle"),
            max_matches=10,
            max_bytes=8192,
            timeout_seconds=0.1,
        )
    )
    if cancel:
        while not processes:
            await asyncio.sleep(0.005)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
    else:
        outcome = await asyncio.wait_for(task, 2)
        assert outcome.error_code == "timeout"
    assert processes and processes[0].returncode is not None


async def test_search_does_not_follow_an_outside_symlink(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("needle secret")
    (workspace / "escape").symlink_to(outside)
    tool = SearchFilesTool()
    with pytest.raises(ValueError, match="escapes"):
        await tool.invoke(ToolContext(workspace), {"query": "needle", "path": "escape"})
    outcome = await tool.invoke(ToolContext(workspace), {"query": "needle"})
    assert not outcome.data["matches"]


async def test_zero_width_matches_advance_across_pages(tmp_path):
    (tmp_path / "a.txt").write_text("hello\nworld\n")
    tool = SearchFilesTool(max_matches=1)
    first = await tool.invoke(ToolContext(tmp_path), {"query": "^"})
    assert first.error_code is None
    assert first.data["locations"][0]["match_start"] == 0
    second = await tool.invoke(
        ToolContext(tmp_path), {"query": "^", "cursor": first.data["next_cursor"]}
    )
    assert second.error_code is None
    assert second.data["locations"][0]["match_start"] == 6
    assert second.data["next_cursor"] is None


async def test_parent_symlink_swap_cannot_escape_during_excerpt_read(
    tmp_path, monkeypatch
):
    from crucible.tools import search_io

    workspace = tmp_path / "workspace"
    directory = workspace / "nested"
    directory.mkdir(parents=True)
    (directory / "a.txt").write_text("needle safe")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "a.txt").write_text("needle SECRET_CANARY")
    original = search_io._snippet

    def swap(root, path, offset, matched):
        directory.rename(workspace / "saved")
        directory.symlink_to(outside, target_is_directory=True)
        return original(root, path, offset, matched)

    monkeypatch.setattr(search_io, "_snippet", swap)
    outcome = await SearchFilesTool().invoke(
        ToolContext(workspace), {"query": "needle"}
    )
    assert outcome.error_code == "search_failed"
    assert "SECRET_CANARY" not in outcome.display_text


async def test_cursor_is_bound_to_search_scope_and_regex_matches_keep_offsets(tmp_path):
    (tmp_path / "a.txt").write_text("prefix needle42 suffix\nneedle7\n")
    tool = SearchFilesTool(max_matches=1)
    first = await tool.invoke(ToolContext(tmp_path), {"query": r"needle\d+"})
    assert first.data["locations"][0]["match_start"] == 7
    assert first.data["locations"][0]["match_end"] == 15
    changed = await tool.invoke(
        ToolContext(tmp_path),
        {"query": r"needle\d+", "path": "a.txt", "cursor": first.data["next_cursor"]},
    )
    assert changed.error_code == "invalid_cursor"


async def test_path_delimiters_and_unicode_offsets_are_not_ambiguous(tmp_path):
    name = "odd:name\nfile.txt"
    text = "🙂 prefix needle suffix"
    (tmp_path / name).write_text(text)
    outcome = await SearchFilesTool().invoke(ToolContext(tmp_path), {"query": "needle"})
    assert outcome.error_code is None
    location = outcome.data["locations"][0]
    assert location["path"] == name
    assert text.encode()[location["match_start"] : location["match_end"]] == b"needle"


async def test_missing_ripgrep_and_exit_one_with_stderr_are_not_no_matches(
    tmp_path, monkeypatch
):
    original = asyncio.create_subprocess_exec

    async def missing(*args, **kwargs):
        raise FileNotFoundError("rg")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", missing)
    unavailable = await SearchFilesTool().invoke(
        ToolContext(tmp_path), {"query": "needle"}
    )
    assert unavailable.error_code == "tool_unavailable"

    async def failed(*args, **kwargs):
        return await original(
            sys.executable,
            "-c",
            'import sys; sys.stderr.write("search launch failed"); sys.exit(1)',
            **kwargs,
        )

    monkeypatch.setattr(asyncio, "create_subprocess_exec", failed)
    failure = await SearchFilesTool().invoke(ToolContext(tmp_path), {"query": "needle"})
    assert failure.error_code == "search_failed"
    assert failure.truncated


async def test_search_ignores_user_config_and_does_not_inherit_credentials(
    tmp_path, monkeypatch
):
    (tmp_path / "a.txt").write_text("needle")
    config = tmp_path / "rg-config"
    config.write_text("--glob=!a.txt\n")
    monkeypatch.setenv("RIPGREP_CONFIG_PATH", str(config))
    monkeypatch.setenv("OPENAI_API_KEY", "SECRET_CANARY")
    original = asyncio.create_subprocess_exec

    async def capture(*args, **kwargs):
        assert "--no-config" in args
        assert set(kwargs["env"]) == {"PATH"}
        return await original(*args, **kwargs)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", capture)
    outcome = await SearchFilesTool().invoke(
        ToolContext(tmp_path), {"query": "needle", "path": "a.txt"}
    )
    assert outcome.error_code is None
    assert outcome.data["matches"]

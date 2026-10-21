"""Role evidence reads are bounded and can advance through single-line traces."""

from pathlib import Path

import pytest

from crucible.improvement.role_worker import role_registry
from crucible.tools.definitions import ToolContext


@pytest.mark.parametrize("kind", ["mining", "format_correction", "proposal"])
async def test_role_chunk_reader_reconstructs_large_utf8_trace(tmp_path: Path, kind):
    text = '{"result":"' + "ab🙂é" * 5000 + '"}'
    (tmp_path / "trace.txt").write_text(text)
    tool = role_registry(kind).get("read_file_chunk")
    assert tool is not None
    offset, chunks = 0, []
    while True:
        result = await tool.invoke(
            ToolContext(tmp_path), {"path": "trace.txt", "offset": offset}
        )
        content = result.data["content"]
        assert isinstance(content, str)
        assert len(content.encode()) <= 8192
        chunks.append(content)
        next_offset = result.data["next_offset"]
        if next_offset is None:
            assert result.truncated is False
            break
        assert next_offset > offset
        assert result.truncated is True
        offset = next_offset
    assert "".join(chunks) == text


async def test_chunk_reader_limits_and_workspace_isolation(tmp_path: Path):
    tool = role_registry("mining").get("read_file_chunk")
    assert tool is not None
    context = ToolContext(tmp_path)
    (tmp_path / "small.txt").write_text("hello")
    for args in (
        {"path": "../private"},
        {"path": "/etc/passwd"},
        {"path": "small.txt", "offset": -1},
        {"path": "small.txt", "limit": 8193},
    ):
        with pytest.raises(ValueError):
            await tool.invoke(context, args)
    result = await tool.invoke(context, {"path": "small.txt", "offset": 100})
    assert result.data["content"] == ""
    assert result.data["next_offset"] is None


async def test_role_legacy_read_file_cannot_dump_large_trace(tmp_path: Path):
    (tmp_path / "trace.txt").write_text("x" * 100_000)
    for kind in ("mining", "format_correction", "proposal"):
        tool = role_registry(kind).get("read_file")
        assert tool is not None
        result = await tool.invoke(ToolContext(tmp_path), {"path": "trace.txt"})
        assert len(result.data["content"].encode()) <= 8192
        assert result.truncated


@pytest.mark.parametrize("size", [0, 8191, 8192, 8193])
async def test_chunk_reader_exact_limits(tmp_path: Path, size: int):
    tool = role_registry("mining").get("read_file_chunk")
    assert tool is not None
    (tmp_path / "trace.txt").write_text("x" * size)
    result = await tool.invoke(ToolContext(tmp_path), {"path": "trace.txt"})
    assert len(result.data["content"]) == min(size, 8192)
    assert result.data["next_offset"] == (8192 if size > 8192 else None)


async def test_chunk_reader_binary_and_symlink_escape(tmp_path: Path):
    tool = role_registry("mining").get("read_file_chunk")
    assert tool is not None
    context = ToolContext(tmp_path)
    (tmp_path / "binary").write_bytes(b"a\x00b")
    result = await tool.invoke(context, {"path": "binary"})
    assert result.error_code == "binary_file"
    (tmp_path / "escape").symlink_to(tmp_path.parent / "private")
    with pytest.raises(ValueError):
        await tool.invoke(context, {"path": "escape"})

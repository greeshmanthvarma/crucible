from pathlib import Path

import pytest

from crucible.context.instructions import load_root_instructions


def test_loads_only_root_agents_and_hashes_exact_bytes(tmp_path: Path) -> None:
    (tmp_path / "AGENTS.md").write_bytes(b"root policy\n")
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "AGENTS.md").write_text("ignored")

    loaded = load_root_instructions(tmp_path)

    assert loaded is not None
    assert loaded.text == "root policy\n"
    assert len(loaded.digest) == 64


def test_rejects_root_agents_symlink_escape(tmp_path: Path) -> None:
    outside = tmp_path.parent / "outside-agents.md"
    outside.write_text("escape")
    (tmp_path / "AGENTS.md").symlink_to(outside)

    with pytest.raises(ValueError, match="escapes"):
        load_root_instructions(tmp_path)

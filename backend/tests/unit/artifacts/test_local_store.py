import stat
from datetime import UTC, datetime

from crucible.artifacts.store import LocalArtifactStore
from crucible.domain.ids import new_id

NOW = datetime(2026, 9, 23, tzinfo=UTC)
CLOCK = type("Clock", (), {"now": lambda self: NOW})()


async def chunks():
    yield b"012345"
    yield b"6789extra"


async def test_store_is_content_addressed_atomic_bounded_and_private(tmp_path) -> None:
    store = LocalArtifactStore(tmp_path, CLOCK)
    artifact = await store.put(
        new_id(), "application/x-ndjson", "private", chunks(), hard_limit=10
    )
    path = tmp_path / artifact.storage_identity

    assert artifact.byte_length == 10
    assert artifact.metadata == {
        "original_bytes": 15,
        "retained_bytes": 10,
        "truncated": True,
    }
    assert stat.S_IMODE(path.stat().st_mode) & 0o077 == 0
    assert await store.read(artifact.storage_identity) == b"0123456789"


async def test_duplicate_content_reuses_content_address(tmp_path) -> None:
    store = LocalArtifactStore(tmp_path, CLOCK)

    async def content():
        yield b"same"

    first = await store.put(new_id(), "text/plain", "private", content())
    second = await store.put(new_id(), "text/plain", "private", content())

    assert first.content_hash == second.content_hash
    assert first.storage_identity == second.storage_identity

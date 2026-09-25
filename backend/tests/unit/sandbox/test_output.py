from crucible.sandbox.output import OutputCapture
from crucible.sandbox.protocol import OutputChunk, OutputStream


async def test_output_fanout_has_independent_live_model_and_artifact_limits() -> None:
    capture = OutputCapture(live_limit=4, model_limit=6, artifact_limit=8)
    await capture.accept(OutputChunk(OutputStream.STDOUT, 1, b"abcdefghij"))

    assert capture.live == b"abcd"
    assert capture.model == b"abcdef"
    assert capture.artifact == b"abcdefgh"
    assert capture.metadata.original_bytes == 10
    assert capture.metadata.live.truncated
    assert capture.metadata.model.retained_bytes == 6
    assert capture.metadata.artifact.retained_bytes == 8


async def test_output_preserves_interleaved_sequence_and_stream_metadata() -> None:
    capture = OutputCapture(live_limit=100, model_limit=100, artifact_limit=100)
    await capture.accept(OutputChunk(OutputStream.STDOUT, 1, b"out"))
    await capture.accept(OutputChunk(OutputStream.STDERR, 2, b"err"))

    records = capture.artifact_records()
    assert [record["sequence"] for record in records] == [1, 2]
    assert [record["stream"] for record in records] == ["stdout", "stderr"]
    assert capture.metadata.stdout_bytes == 3
    assert capture.metadata.stderr_bytes == 3

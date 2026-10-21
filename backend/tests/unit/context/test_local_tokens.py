from datetime import timedelta
from uuid import uuid4

import pytest

from crucible.engine.gateway import (
    ModelMessage,
    ModelPart,
    ModelRole,
    ModelToolDefinition,
    PreparedModelRequest,
)
from crucible.models.token_counting import LocalTokenEstimator, estimate_input_tokens


def test_context_and_reservation_count_identical_complete_payloads() -> None:
    tools = (ModelToolDefinition("read_file", "read safely", {"type": "object"}),)
    messages = (
        ModelMessage(ModelRole.SYSTEM, (ModelPart("text", "instructions"),)),
        ModelMessage(
            ModelRole.ASSISTANT,
            (
                ModelPart(
                    "tool_call",
                    tool_name="read_file",
                    tool_call_id=uuid4(),
                    arguments={"path": "trace.json"},
                ),
            ),
        ),
        ModelMessage(
            ModelRole.TOOL,
            (
                ModelPart(
                    "tool_result", "0123456789abcdef " * 5000, tool_call_id=uuid4()
                ),
            ),
        ),
    )
    request = PreparedModelRequest(
        uuid4(), uuid4(), "openai/gpt-6-luna", messages, tools, 4096
    )
    assert LocalTokenEstimator(tools=tools).estimate(messages) == estimate_input_tokens(
        request.messages, request.tools
    )
    assert (
        estimate_input_tokens(messages, tools)
        > len(messages[-1].parts[0].text_content) // 4
    )
    assert estimate_input_tokens(messages, tools) > estimate_input_tokens(messages, ())


@pytest.mark.parametrize(
    "text", ["中文測試" * 100, "0123456789abcdef " * 100, "<|endoftext|>"]
)
def test_tokenization_accepts_unicode_code_and_special_token_like_text(
    text: str,
) -> None:
    messages = (ModelMessage(ModelRole.USER, (ModelPart("text", text),)),)
    assert estimate_input_tokens(messages, ()) > 0


def test_offline_encoding_matches_pinned_tiktoken(monkeypatch) -> None:
    import tiktoken

    from crucible.models.token_counting import _encoding, asset_directory

    monkeypatch.setenv("TIKTOKEN_CACHE_DIR", asset_directory())
    reference = tiktoken.get_encoding("o200k_base")
    _encoding.cache_clear()

    # Runtime must not invoke the library loader, even with an empty memory cache.
    def forbidden_loader(*args, **kwargs):
        raise AssertionError("No runtime asset loader allowed")

    monkeypatch.setattr(tiktoken, "get_encoding", forbidden_loader)
    local = _encoding()
    for sample in (
        "hello world",
        "中文測試",
        "0123456789abcdef",
        "we'll read the file\n\n",
        "<|endoftext|>",
        " x/\n\t",
    ):
        assert local.encode_ordinary(sample) == reference.encode_ordinary(sample)


def test_tool_arguments_are_counted_without_text_messages() -> None:
    def messages(path):
        return (
            ModelMessage(
                ModelRole.ASSISTANT,
                (
                    ModelPart(
                        "tool_call",
                        tool_call_id=uuid4(),
                        tool_name="read_file",
                        arguments={"path": path},
                    ),
                ),
            ),
        )

    assert estimate_input_tokens(messages("中文" * 5000), ()) > estimate_input_tokens(
        messages("a"), ()
    )


def test_missing_assets_fail_without_a_network_fallback(tmp_path, monkeypatch) -> None:
    from crucible.models.token_counting import _encoding

    _encoding.cache_clear()
    monkeypatch.setenv("TIKTOKEN_CACHE_DIR", str(tmp_path))
    with pytest.raises(ValueError, match="tokenizer assets"):
        estimate_input_tokens((), ())
    _encoding.cache_clear()


def test_corrupt_assets_fail_without_network(tmp_path, monkeypatch) -> None:
    import hashlib

    from crucible.models.token_counting import ASSET_URL, _encoding

    monkeypatch.setenv("TIKTOKEN_CACHE_DIR", str(tmp_path))
    (tmp_path / hashlib.sha1(ASSET_URL.encode()).hexdigest()).write_bytes(b"corrupt")
    _encoding.cache_clear()
    with pytest.raises(ValueError, match="integrity"):
        estimate_input_tokens((), ())
    _encoding.cache_clear()


async def test_gateway_reserves_shared_local_count_plus_output_allowance() -> None:
    from crucible.domain.clock import SystemClock
    from crucible.repair.gateway import ScopedModelGateway

    class NeverCalled:
        async def stream(self, request):
            raise AssertionError("No model calls allowed")
            yield

    gateway = ScopedModelGateway(NeverCalled(), clock=SystemClock(), endpoint="local")
    job = uuid4()
    credential = gateway.issue(
        job, "model", {"max_requests": 1, "max_tokens": 1000000}, timedelta(minutes=1)
    )
    messages = (
        ModelMessage(ModelRole.USER, (ModelPart("text", "中文0123456789" * 100),)),
    )
    request = PreparedModelRequest(uuid4(), uuid4(), "model", messages, (), 4096)
    reservation = await gateway.reserve(credential.token, job, request)
    assert reservation.input_estimate == LocalTokenEstimator().estimate(messages)
    assert reservation.tokens == reservation.input_estimate + 4096

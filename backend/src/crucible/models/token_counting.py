"""Local o200k policy: full wire payload plus 20% and framing headroom.

This is a conservative estimate, never an exact provider billing count. Assets
must be prepared at install/image-build time; runtime never downloads them.
"""

import hashlib
import json
import os
from functools import lru_cache
from pathlib import Path

import tiktoken

from crucible.engine.gateway import ModelMessage, ModelToolDefinition
from crucible.models.request_payload import response_items, response_tool

ENCODING = "o200k_base"
ASSET_URL = "https://openaipublic.blob.core.windows.net/encodings/o200k_base.tiktoken"
ASSET_HASH = "446a9538cb6c348e3516120d7c08b09f57c36495e2acfffe59a5bf8b0cfb1a2d"
VERSION = "tiktoken-0.14.0-o200k-wire-plus20pct-framing-v1"


def asset_directory() -> str:
    return os.environ.get(
        "TIKTOKEN_CACHE_DIR",
        os.environ.get(
            "DATA_GYM_CACHE_DIR", str(Path.home() / ".cache/crucible/tiktoken")
        ),
    )


@lru_cache(maxsize=1)
def _encoding() -> tiktoken.Encoding:
    directory = asset_directory()
    if not directory:
        raise ValueError("Local tokenizer assets require an enabled cache")
    path = Path(directory) / hashlib.sha1(ASSET_URL.encode()).hexdigest()
    try:
        raw = path.read_bytes()
    except OSError as error:
        raise ValueError(
            "Local tokenizer assets missing; run tokenizer preparation"
        ) from error
    if hashlib.sha256(raw).hexdigest() != ASSET_HASH:
        raise ValueError("Local tokenizer assets failed integrity verification")
    # Decode the verified local rank file without invoking tiktoken's URL/cache
    # loader. Even missing/corrupt assets cannot cause a runtime network request.
    import base64

    ranks = {
        base64.b64decode(token): int(rank)
        for token, rank in (line.split() for line in raw.splitlines() if line)
    }
    pattern = "|".join(
        [
            r"[^\r\n\p{L}\p{N}]?[\p{Lu}\p{Lt}\p{Lm}\p{Lo}\p{M}]*[\p{Ll}\p{Lm}\p{Lo}\p{M}]+(?i:'s|'t|'re|'ve|'m|'ll|'d)?",
            r"[^\r\n\p{L}\p{N}]?[\p{Lu}\p{Lt}\p{Lm}\p{Lo}\p{M}]+[\p{Ll}\p{Lm}\p{Lo}\p{M}]*(?i:'s|'t|'re|'ve|'m|'ll|'d)?",
            r"\p{N}{1,3}",
            r" ?[^\s\p{L}\p{N}]+[\r\n/]*",
            r"\s*[\r\n]+",
            r"\s+(?!\S)",
            r"\s+",
        ]
    )
    return tiktoken.Encoding(
        name=ENCODING,
        pat_str=pattern,
        mergeable_ranks=ranks,
        special_tokens={"<|endoftext|>": 199999, "<|endofprompt|>": 200018},
    )


def estimate_input_tokens(
    messages: tuple[ModelMessage, ...], tools: tuple[ModelToolDefinition, ...]
) -> int:
    items = [item for message in messages for item in response_items(message)]
    payload = json.dumps(
        {"input": items, "tools": [response_tool(t) for t in tools]},
        ensure_ascii=False,
        separators=(",", ":"),
    )
    tokens = len(_encoding().encode_ordinary(payload))
    return (tokens * 6 + 4) // 5 + 16 * (len(items) + len(tools)) + 16


class LocalTokenEstimator:
    version = VERSION

    def __init__(self, *, tools: tuple[ModelToolDefinition, ...] = ()) -> None:
        self.tools = tools

    def estimate(self, messages: tuple[ModelMessage, ...]) -> int:
        return estimate_input_tokens(messages, self.tools)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Prepare the local tokenizer vocabulary"
    )
    parser.add_argument("--prepare", action="store_true", required=True)
    parser.parse_args()
    # Explicit install-time command only. Runtime counting never uses this loader.
    os.environ["TIKTOKEN_CACHE_DIR"] = asset_directory()
    tiktoken.get_encoding(ENCODING)
    _encoding()
    print("Local tokenizer vocabulary prepared and hash-verified.")

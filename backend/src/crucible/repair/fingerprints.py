"""Versioned comparison identities; execution UUIDs are provenance only."""

import asyncio
from pathlib import Path
from typing import Literal

from pydantic import Field, JsonValue

from crucible.repair.cases import StrictModel
from crucible.repair.identity import digest


class SourceIdentity(StrictModel):
    commit: str
    tree_digest: str


class EvaluationIdentity(StrictModel):
    schema_version: Literal[1] = 1
    source: SourceIdentity
    model: str
    parameters: dict[str, JsonValue]
    fixture_digest: str
    eval_digest: str
    instruction_digest: str
    tool_schema_digest: str
    prompt_digest: str
    lock_digest: str
    image_digest: str
    verifier_version: str
    estimator_version: str
    adapter_version: str
    resource_policy: dict[str, JsonValue]
    budgets: dict[str, JsonValue]
    repeats: int = Field(gt=0, strict=True)


def fingerprint(identity: EvaluationIdentity) -> str:
    validated = EvaluationIdentity.model_validate(identity.model_dump(mode="json"))
    return digest(validated.model_dump(mode="json"))


async def capture_source_identity(root: "Path") -> SourceIdentity:
    """Hash current admitted source, including untracked additions and deletions.

    RuntimeIdentity remains the frozen startup identity. This is the separate
    per-evaluation snapshot identity; the supervisor must freeze the corresponding
    export and reject drift before dispatch.
    """
    from crucible.repair.identity import _git, _read_tracked

    root = Path(
        (await _git(root, "rev-parse", "--show-toplevel")).decode().strip()
    ).resolve()
    commit = (await _git(root, "rev-parse", "HEAD")).decode().strip()
    names = (
        (
            await _git(
                root, "ls-files", "--cached", "--others", "--exclude-standard", "-z"
            )
        )
        .decode()
        .split("\0")
    )
    included = sorted(
        {
            name
            for name in names
            if name
            and not any(
                part == ".env" or part.startswith(".env.") for part in Path(name).parts
            )
        }
    )
    # --exclude-standard filters untracked files; explicitly filter tracked ignored
    # entries too, without ever reading their private content.
    process = await asyncio.create_subprocess_exec(
        "git",
        "check-ignore",
        "--no-index",
        "-z",
        "--stdin",
        cwd=root,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    output, _ = await process.communicate(
        "\0".join(included).encode() + (b"\0" if included else b"")
    )
    if process.returncode not in (0, 1):
        raise ValueError("Cannot filter private source inputs")
    ignored = set(output.decode().split("\0"))
    files = await asyncio.to_thread(
        lambda: [_read_tracked(root, name) for name in included if name not in ignored]
    )
    if any(file.get("stable") is False or "unsupported" in file for file in files):
        raise ValueError("Source snapshot is unstable or unreadable")
    return SourceIdentity(
        commit=commit, tree_digest=digest({"schema_version": 1, "files": files})
    )

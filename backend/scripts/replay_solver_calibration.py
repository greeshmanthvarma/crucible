"""Replay harness costs with fixed recorded behavior; never invoke a model."""

import argparse
import asyncio
import json
import subprocess
import tempfile
from collections import Counter, defaultdict
from dataclasses import replace
from pathlib import Path
from statistics import mean, median
from uuid import UUID

from crucible.context.instructions import load_root_instructions
from crucible.context.invariants import SimpleTokenEstimator
from crucible.context.result_projection import project_coding_tool_results
from crucible.engine.coding_behavior import BehaviorObservation
from crucible.engine.gateway import ModelMessage, ModelPart, ModelRole
from crucible.tools.behavior_contracts import normalize_unified_diff
from crucible.tools.definitions import ToolContext
from crucible.tools.filesystem import ListFilesTool
from crucible.tools.workspace_authority import patch_paths

BUDGET = 200_000


def git(
    workspace: Path, *args: str, patch: str | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(workspace), *args],
        input=patch,
        capture_output=True,
        text=True,
        check=False,
    )


def check_patches(workspace: Path, patches: list[str]) -> Counter[str]:
    counts: Counter[str] = Counter()
    if not patches:
        return counts
    # Require this workspace's own repository, rather than an ancestor's HEAD.
    head = (
        git(workspace, "rev-parse", "--verify", "HEAD")
        if (workspace / ".git").exists()
        else None
    )
    if head is None or head.returncode:
        counts["unchecked"] = len(patches)
        return counts
    with tempfile.TemporaryDirectory(prefix="crucible-replay-") as temporary:
        checkout = Path(temporary) / "checkout"
        cloned = subprocess.run(
            [
                "git",
                "clone",
                "--quiet",
                "--shared",
                "--no-checkout",
                str(workspace),
                str(checkout),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if (
            cloned.returncode
            or git(
                checkout,
                "-c",
                "core.hooksPath=/dev/null",
                "checkout",
                "--quiet",
                "--detach",
                head.stdout.strip(),
            ).returncode
        ):
            counts["unchecked"] = len(patches)
            return counts
        for patch in patches:
            checked = git(checkout, "apply", "--check", "--", patch=patch)
            counts[
                "git_apply_pass" if checked.returncode == 0 else "git_apply_fail"
            ] += 1
    return counts


def trace_messages(data: dict) -> tuple[ModelMessage, ...]:
    calls = {call["id"]: call for call in data["tool_calls"]}
    results = {result["id"]: result for result in data["tool_results"]}
    messages = []
    for message in sorted(
        data["messages"], key=lambda item: item["conversation_sequence"]
    ):
        parts = []
        for part in message["parts"]:
            result = results.get(part.get("tool_result_id"))
            call_id = part.get("tool_call_id") or (
                result["tool_call_id"] if result else None
            )
            call = calls.get(call_id)
            parts.append(
                ModelPart(
                    part["kind"],
                    part.get("text_content") or part.get("reasoning_content"),
                    tool_call_id=UUID(call_id) if call_id else None,
                    tool_name=call["name"] if call else None,
                    arguments=call["arguments"] if call else None,
                    provider_correlation_id=call.get("provider_correlation_id")
                    if call
                    else None,
                )
            )
        messages.append(ModelMessage(ModelRole(message["role"]), tuple(parts)))
    return tuple(messages)


def requests_that_fit(estimates: list[int]) -> int:
    total = 0
    for index, estimate in enumerate(estimates):
        total += estimate
        if total > BUDGET:
            return index
    return len(estimates)


def token_replay(
    messages: tuple[ModelMessage, ...], opening_id: UUID, listing: str, workspace: Path
) -> dict:
    instructions = [
        "Follow harness safety and execution policy.",
        "Use only the structured tools supplied by the harness.",
    ]
    repository = load_root_instructions(workspace)
    if repository is not None:
        instructions.append(repository.text)
    prefix = tuple(
        ModelMessage(ModelRole.SYSTEM, (ModelPart("text", text),))
        for text in instructions
    )
    estimator = SimpleTokenEstimator()
    before = []
    after = []
    for index, message in enumerate(messages):
        if message.role is not ModelRole.ASSISTANT:
            continue
        original = prefix + messages[:index]
        updated = tuple(
            replace(
                m,
                parts=tuple(
                    replace(p, text_content=listing)
                    if p.kind == "tool_result" and p.tool_call_id == opening_id
                    else p
                    for p in m.parts
                ),
            )
            for m in original
        )
        observation = BehaviorObservation(len(before) + 1, 0, 0, BUDGET, 0, (), False)
        projected = project_coding_tool_results(updated, observation, 0, 0, estimator)
        before.append(estimator.estimate(original))
        after.append(estimator.estimate(projected))
    return {
        "input_estimates_before": before,
        "input_estimates_after": after,
        "total_before": sum(before),
        "total_after": sum(after),
        "ratio": sum(after) / sum(before),
        "requests_that_fit_before": requests_that_fit(before),
        "requests_that_fit_after": requests_that_fit(after),
        "recorded_requests": len(before),
    }


async def replay(root: Path) -> dict:
    patches: Counter[str] = Counter(
        dict.fromkeys(
            (
                "failed",
                "invalid_arguments",
                "raw_parseable",
                "normalized_parseable",
                "recovered_by_normalization",
                "still_rejected",
                "git_apply_pass",
                "git_apply_fail",
                "unchecked",
            ),
            0,
        )
    )
    listings: dict[str, list[dict]] = defaultdict(list)
    tokens = []
    traces = sorted(root.glob("*/trace.json"))
    if not traces:
        raise ValueError(f"No trial traces in {root}")
    for trace in traces:
        data = json.loads(trace.read_text())["data"]
        request = json.loads(trace.with_name("request.json").read_text())
        workspace = trace.parent / "workspace"
        calls = {call["id"]: call for call in data["tool_calls"]}
        checkable = []
        for result in data["tool_results"]:
            call = calls[result["tool_call_id"]]
            if call["name"] != "apply_patch" or result["status"] == "succeeded":
                continue
            patches["failed"] += 1
            raw = call["arguments"].get("patch")
            if not isinstance(raw, str):
                patches["invalid_arguments"] += 1
                continue
            try:
                patch_paths(raw)
                raw_valid = True
                patches["raw_parseable"] += 1
            except ValueError:
                raw_valid = False
            normalized = normalize_unified_diff(raw)
            try:
                patch_paths(normalized)
            except ValueError:
                patches["still_rejected"] += 1
                continue
            patches["normalized_parseable"] += 1
            if not raw_valid:
                patches["recovered_by_normalization"] += 1
            checkable.append(normalized)
        patches.update(check_patches(workspace, checkable))
        opening = next(c for c in data["tool_calls"] if c["name"] == "list_files")
        old = next(
            r["display_text"]
            for r in data["tool_results"]
            if r["tool_call_id"] == opening["id"]
        )
        new = await ListFilesTool().invoke(ToolContext(workspace), opening["arguments"])
        listings[request["identity"]["case_digest"]].append(
            {
                "trial_id": trace.parent.name,
                "before_chars": len(old),
                "after_chars": len(new.display_text),
                "before_bytes": len(old.encode()),
                "after_bytes": len(new.display_text.encode()),
            }
        )
        tokens.append(
            {
                "trial_id": trace.parent.name,
                **token_replay(
                    trace_messages(data),
                    UUID(opening["id"]),
                    new.display_text,
                    workspace,
                ),
            }
        )
    all_listings = [item for rows in listings.values() for item in rows]
    return {
        "trials": len(traces),
        "method": (
            "Fixed observed trajectory; SimpleTokenEstimator text-only input estimates "
            "include harness and root instructions. Tool schemas/arguments and output "
            "tokens are excluded. Requests that fit are capped at recorded requests; "
            "no future behavior is extrapolated. Git checks use independent clean "
            "checkouts of each trial workspace HEAD, with no preceding trial edits."
        ),
        "patches": dict(patches),
        "listing": {
            "mean_before_chars": mean(r["before_chars"] for r in all_listings),
            "mean_after_chars": mean(r["after_chars"] for r in all_listings),
            "cases": dict(listings),
        },
        "tokens": {
            "median_ratio": median(t["ratio"] for t in tokens),
            "median_total_before": median(t["total_before"] for t in tokens),
            "median_total_after": median(t["total_after"] for t in tokens),
            "median_requests_that_fit_before": median(
                t["requests_that_fit_before"] for t in tokens
            ),
            "median_requests_that_fit_after": median(
                t["requests_that_fit_after"] for t in tokens
            ),
            "requests_that_fit_before": sum(
                t["requests_that_fit_before"] for t in tokens
            ),
            "requests_that_fit_after": sum(
                t["requests_that_fit_after"] for t in tokens
            ),
            "per_trial": tokens,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trials_dir", type=Path)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(replay(args.trials_dir.resolve())), indent=2))


if __name__ == "__main__":
    main()

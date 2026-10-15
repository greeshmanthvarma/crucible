"""Host-owned deterministic assertions over stopped-worker observations."""

import hashlib
import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

from crucible.context.invariants import SimpleTokenEstimator
from crucible.engine.gateway import ModelMessage, ModelPart, ModelRole
from crucible.repair.cases import SUMMARY_FIELDS, Assertion, IncidentCase, relative_path

VERIFIER_VERSION = "incident-assertions-v1"
_MISSING = object()


@dataclass(frozen=True)
class AssertionResult:
    op: str
    passed: bool
    detail: str


def _read(root: Path, name: str) -> bytes | None:
    """Traverse using directory descriptors; never follow workspace symlinks."""
    parts = relative_path(name).split("/")
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for component in parts[:-1]:
            child = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = child
        try:
            file_descriptor = os.open(
                parts[-1],
                os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                dir_fd=descriptor,
            )
        except FileNotFoundError:
            return None
        with os.fdopen(file_descriptor, "rb") as source:
            if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
                raise ValueError("Fixture entry is not a regular file")
            return source.read()
    except FileNotFoundError:
        return None
    finally:
        os.close(descriptor)


def _inventory(root: Path) -> dict[str, tuple[str, int]]:
    inventory = {}
    for parent, directories, files in os.walk(root, followlinks=False):
        for name in directories + files:
            path = Path(parent) / name
            metadata = path.lstat()
            if stat.S_ISLNK(metadata.st_mode):
                raise ValueError("Symlink in workspace snapshot")
            if stat.S_ISREG(metadata.st_mode):
                relative = path.relative_to(root).as_posix()
                content = _read(root, relative)
                assert content is not None
                inventory[relative] = (
                    hashlib.sha256(content).hexdigest(),
                    stat.S_IMODE(metadata.st_mode),
                )
            elif not stat.S_ISDIR(metadata.st_mode):
                raise ValueError("Nonregular workspace entry")
    return inventory


def _messages(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise ValueError("Missing normalized context messages")
    for message in value:
        if not isinstance(message, dict) or not isinstance(message.get("parts"), list):
            raise ValueError("Invalid context message")
        ModelRole(message["role"])
        if any(
            not isinstance(part, dict) or not isinstance(part.get("kind"), str)
            for part in message["parts"]
        ):
            raise ValueError("Invalid context part")
    return value


def _pairs(messages: list[dict[str, Any]]) -> bool:
    calls: set[str] = set()
    results: set[str] = set()
    for message in messages:
        for part in message["parts"]:
            if part["kind"] not in {"tool_call", "tool_result"}:
                continue
            identity = part.get("tool_call_id")
            if not isinstance(identity, str) or not identity:
                return False
            if part["kind"] == "tool_call":
                if identity in calls or message["role"] != "assistant":
                    return False
                calls.add(identity)
            else:
                if identity not in calls or identity in results:
                    return False
                results.add(identity)
    return calls == results


def _ordered(selected: list[dict[str, Any]], source: list[dict[str, Any]]) -> bool:
    sequences = [message["conversation_sequence"] for message in source]
    if any(type(sequence) is not int for sequence in sequences) or sequences != sorted(
        set(sequences)
    ):
        return False
    cursor = 0
    for message in selected:
        while cursor < len(source) and source[cursor] != message:
            cursor += 1
        if cursor == len(source):
            return False
        cursor += 1
    return True


def _summary(value: object) -> dict[str, str]:
    if (
        not isinstance(value, dict)
        or set(value) != SUMMARY_FIELDS
        or any(not isinstance(item, str) for item in value.values())
    ):
        raise ValueError("Summary does not match the fixed string-field schema")
    return value


def _assert(
    assertion: Assertion,
    observed: dict[str, Any],
    before: Path,
    after: Path,
    settings: dict[str, Any],
) -> tuple[bool, str]:
    op, value = assertion.op, assertion.value
    result = observed.get("tool_result")
    if op == "error_code_equals":
        return observed["error_code"] == value, "Observed error code compared"
    if op == "status_equals":
        return observed["status"] == value, "Observed status compared"
    if op.startswith("output_text_"):
        text = result.get("output_text") if isinstance(result, dict) else None
        return isinstance(text, str) and (
            text == value
            if op.endswith("equals")
            else isinstance(value, str) and value in text
        ), "Literal output compared"
    if op == "result_field_equals":
        current: Any = result
        assert assertion.path is not None
        for encoded in assertion.path[1:].split("/"):
            segment = encoded.replace("~1", "/").replace("~0", "~")
            if isinstance(current, dict):
                current = current.get(segment, _MISSING)
            elif (
                isinstance(current, list)
                and segment.isascii()
                and segment.isdecimal()
                and str(int(segment)) == segment
                and int(segment) < len(current)
            ):
                current = current[int(segment)]
            else:
                current = _MISSING
                break
        # Python treats True == 1; JSON types must remain distinct.
        return current is not _MISSING and json.dumps(
            current, sort_keys=True, ensure_ascii=False
        ) == json.dumps(
            value, sort_keys=True, ensure_ascii=False
        ), "Literal JSON field compared"
    if op.startswith("file_"):
        assert assertion.path is not None
        content = _read(after, assertion.path)
        if op == "file_exists":
            return content is not None, "File existence inspected"
        if op == "file_absent":
            return content is None, "File absence inspected"
        if content is None:
            return False, "Required file missing"
        text = content.decode("utf-8")
        return text == value if op == "file_text_equals" else isinstance(
            value, str
        ) and value in text, "Exact UTF-8 text inspected"
    if op == "unrelated_files_unchanged":
        admitted = settings.get("admitted_mutation_paths", [])
        if not isinstance(admitted, list) or any(
            not isinstance(path, str) for path in admitted
        ):
            raise ValueError("Invalid admitted mutation paths")
        allowed = {relative_path(path) for path in admitted}
        original, changed = _inventory(before), _inventory(after)
        return all(
            original.get(path) == changed.get(path)
            for path in original.keys() | changed.keys()
            if path not in allowed
        ), "Complete fixture and new files compared"
    if op in {"summary_schema_valid", "required_literal_fact_present"}:
        summary = _summary(observed.get("summary"))
        return op == "summary_schema_valid" or isinstance(value, str) and any(
            value in summary[field] for field in assertion.fields
        ), "Fixed summary fields inspected"
    selected = _messages(observed.get("context_messages"))
    if op == "tool_pairs_complete":
        return _pairs(selected), "Tool exchange identities inspected"
    if op == "retained_messages_ordered":
        return _ordered(
            selected, _messages(settings.get("source_messages"))
        ), "Ordered source subsequence inspected"
    if op == "context_within_estimated_limit":
        prepared = tuple(
            ModelMessage(
                ModelRole(message["role"]),
                tuple(
                    ModelPart(
                        part["kind"], part.get("text_content") or part.get("text")
                    )
                    for part in message["parts"]
                ),
            )
            for message in selected
        )
        limit, reserve, threshold = (
            settings.get("input_limit"),
            settings.get("output_reserve", 0),
            settings.get("threshold", 1.0),
        )
        if (
            type(limit) is not int
            or type(reserve) is not int
            or not isinstance(threshold, (int, float))
            or limit <= 0
            or reserve < 0
            or not 0 < threshold <= 1
        ):
            raise ValueError("Invalid protected context capacity settings")
        estimate = SimpleTokenEstimator().estimate(prepared)
        capacity = int(limit * threshold) - reserve
        return (
            estimate <= capacity,
            (
                f"{SimpleTokenEstimator.version}: estimate={estimate}, "
                f"capacity={capacity} (approximate)"
            ),
        )
    raise ValueError("Unsupported assertion operator")


def _observation(value: object) -> dict[str, Any]:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    if not isinstance(value, dict):
        raise ValueError("Expected an observation object")
    if (
        len(json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8"))
        > 1_048_576
    ):
        raise ValueError("Observation exceeds the protected byte limit")
    required = {
        "schema_version",
        "job_id",
        "status",
        "tool_result",
        "context_messages",
        "summary",
        "error_code",
        "detail",
        "artifact_refs",
        "command_records",
    }
    if (
        set(value) != required
        or type(value["schema_version"]) is not int
        or value["schema_version"] != 1
    ):
        raise ValueError("Invalid observation envelope")
    UUID(str(value["job_id"]))
    if value["status"] not in {"completed", "unsupported", "error"}:
        raise ValueError("Invalid observation status")
    for name in ("error_code", "detail"):
        if value[name] is not None and not isinstance(value[name], str):
            raise ValueError("Invalid observation error")
    if value["tool_result"] is not None and not isinstance(value["tool_result"], dict):
        raise ValueError("Invalid tool observation")
    if not isinstance(value["artifact_refs"], (tuple, list)) or not all(
        isinstance(ref, str) for ref in value["artifact_refs"]
    ):
        raise ValueError("Invalid artifact references")
    if not isinstance(value["command_records"], list):
        raise ValueError("Invalid command records")
    return value


def verify_observation(
    case: IncidentCase, observation: object, before: Path, after: Path, phase: str
) -> tuple[AssertionResult, ...]:
    if phase not in {"baseline", "candidate", "regression"}:
        raise ValueError("Unknown verification phase")
    try:
        observed = _observation(observation)
    except (ValueError, TypeError, KeyError):
        return (
            AssertionResult("observation_valid", False, "Invalid worker observation"),
        )
    if phase == "baseline":
        matcher = case.expected_baseline_failure
        detail = observed["detail"] or ""
        # Only supplied controller-owned workspace roots are normalized.
        for root in sorted({str(before), str(after)}, key=len, reverse=True):
            detail = detail.replace(root + "/", "{workspace}/")
        matched = (
            observed["status"] == matcher.status
            and observed["error_code"] == matcher.error_code
            and all(fragment in detail for fragment in matcher.detail_contains)
        )
        return (
            AssertionResult(
                "baseline_failure_matches",
                matched,
                "Structured original failure compared",
            ),
        )
    if observed["status"] != "completed":
        return (
            AssertionResult(
                "execution_completed",
                False,
                "Worker error/unsupported is not candidate success",
            ),
        )
    results = []
    for assertion in case.candidate_assertions:
        try:
            passed, detail = _assert(assertion, observed, before, after, case.settings)
        except (ValueError, OSError, KeyError, TypeError, UnicodeError):
            passed, detail = (
                False,
                "Required observation or fixture is missing, invalid, or unsafe",
            )
        results.append(AssertionResult(assertion.op, passed, detail))
    return tuple(results)

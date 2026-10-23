"""Bounded read-only ripgrep transport and ownership-contained snippet reads."""

import asyncio
import hashlib
import json
import os
import stat
from pathlib import Path

from crucible.tools.definitions import SearchCursor, SearchFilesArguments, ToolOutcome
from crucible.tools.workspace_authority import WorkspacePathResolver


class SearchFailure(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _open_contained(root: Path, path: Path) -> int:
    """Open each directory relative to the root fd, rejecting symlink swaps."""
    parts = path.relative_to(root).parts
    parent = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in parts[:-1]:
            child = os.open(
                part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent
            )
            os.close(parent)
            parent = child
        return os.open(
            parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent
        )
    finally:
        os.close(parent)


def _snippet(
    root: Path, path: Path, offset: int, matched: bytes
) -> tuple[str, int, int, bool]:
    descriptor = _open_contained(root, path)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise SearchFailure("search_failed", "Search target is not a regular file")
        if os.pread(descriptor, min(512, len(matched)), offset) != matched[:512]:
            raise SearchFailure(
                "search_changed", "File changed during search; search again"
            )
        start = max(0, offset - 80)
        raw = os.pread(descriptor, 512, start)
        text = raw.decode("utf-8", errors="ignore")
        # Only trim partial characters at window edges, never silently alter bytes.
        encoded = text.encode()
        trim = raw.find(encoded)
        if trim < 0:
            raise SearchFailure("search_failed", "Search excerpt is not UTF-8 text")
        after = os.fstat(descriptor)
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise SearchFailure(
                "search_changed", "File changed during search; search again"
            )
        start += trim
        end = start + len(encoded)
        return text, start, end, end < offset + len(matched)
    finally:
        os.close(descriptor)


def _outcome(
    hits: list[tuple[str, dict[str, object]]], cursor: SearchCursor | None
) -> ToolOutcome:
    limited = cursor is not None
    text = "\n".join(hit[0] for hit in hits) or "No matches after this cursor."
    if cursor is not None:
        text += (
            "\nMore matches may remain. Continue with cursor="
            + cursor.model_dump_json()
        )
    data = dict(
        matches=[hit[0] for hit in hits],
        locations=[hit[1] for hit in hits],
        truncated=limited,
        next_cursor=cursor.model_dump() if cursor else None,
    )
    return ToolOutcome(data, text, limited)


def _size(outcome: ToolOutcome) -> int:
    return len(json.dumps(dict(outcome.data), ensure_ascii=False).encode()) + len(
        outcome.display_text.encode()
    )


async def _stderr(reader: asyncio.StreamReader) -> bytes:
    saved = bytearray()
    while chunk := await reader.read(4096):
        saved.extend(chunk[: max(0, 4096 - len(saved))])
    return bytes(saved)


async def _discard(reader: asyncio.StreamReader) -> None:
    while await reader.read(65_536):
        pass


def _failure(code: str, message: str, max_bytes: int) -> ToolOutcome:
    bounded = message.encode()[: max(0, max_bytes - 2)].decode("utf-8", errors="ignore")
    return ToolOutcome({}, bounded, True, error_code=code)


async def search_workspace(
    workspace: Path,
    arguments: SearchFilesArguments,
    *,
    max_matches: int,
    max_bytes: int,
    timeout_seconds: float = 10,
    max_scan_bytes: int = 4_194_304,
) -> ToolOutcome:
    if max_matches < 1 or max_bytes < 128 or max_scan_bytes < 1:
        raise ValueError("Search limits must allow a bounded response")
    resolver = WorkspacePathResolver(workspace)
    target = resolver.resolve(arguments.path)
    scope = target.relative_to(resolver.root).as_posix()
    digest = hashlib.sha256((arguments.query + "\0" + scope).encode()).hexdigest()
    cursor = arguments.cursor
    if cursor and cursor.query_digest != digest:
        return _failure(
            "invalid_cursor", "Cursor belongs to a different query or path", max_bytes
        )
    try:
        process = await asyncio.create_subprocess_exec(
            "rg",
            "--no-config",
            "--no-mmap",
            "--regex-size-limit=10M",
            "--dfa-size-limit=10M",
            "--null",
            "--with-filename",
            "--line-number",
            "--byte-offset",
            "--only-matching",
            "--sort",
            "path",
            "--no-heading",
            "--color=never",
            "--glob=!.git",
            "--glob=!.crucible",
            "--",
            arguments.query,
            str(target),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=65_536,
            env={"PATH": os.environ.get("PATH", os.defpath)},
        )
    except FileNotFoundError:
        return _failure("tool_unavailable", "ripgrep is unavailable", max_bytes)
    assert process.stdout and process.stderr
    errors = asyncio.create_task(_stderr(process.stderr))
    hits: list[tuple[str, dict[str, object]]] = []
    last: SearchCursor | None = None
    limited = False
    scanned = 0
    try:
        async with asyncio.timeout(timeout_seconds):
            while True:
                try:
                    raw_path = await process.stdout.readuntil(b"\0")
                except asyncio.IncompleteReadError as error:
                    if error.partial:
                        raise SearchFailure(
                            "search_failed", "Incomplete search record"
                        ) from error
                    break
                record = await process.stdout.readuntil(b"\n")
                scanned += len(raw_path) + len(record)
                if scanned > max_scan_bytes:
                    raise SearchFailure(
                        "search_output_limit",
                        "Search scan-output limit reached; narrow the query or path",
                    )
                file = Path(os.fsdecode(raw_path[:-1]))
                relative = file.relative_to(resolver.root).as_posix()
                line_raw, offset_raw, matched = record[:-1].split(b":", 2)
                offset, line = int(offset_raw), int(line_raw)
                if cursor and (
                    relative < cursor.path
                    or (relative == cursor.path and offset < cursor.byte_offset)
                ):
                    continue
                if len(hits) >= max_matches:
                    limited = True
                    break
                contained = resolver.resolve(relative)
                text, start, end, snippet_truncated = _snippet(
                    resolver.root, contained, offset, matched
                )
                location: dict[str, object] = dict(
                    path=relative,
                    line=line,
                    match_start=offset,
                    match_end=offset + len(matched),
                    snippet_start=start,
                    snippet_end=end,
                    snippet_truncated=snippet_truncated,
                )
                rendered = (
                    f"{relative}:{line}:bytes {offset}-{offset + len(matched)}: {text}"
                )
                next_cursor = SearchCursor(
                    path=relative,
                    byte_offset=max(offset + len(matched), offset + 1),
                    query_digest=digest,
                )
                candidate = hits + [(rendered, location)]
                # Reserve cursor/footer overhead even if this later proves the last hit.
                if _size(_outcome(candidate, next_cursor)) > max_bytes:
                    if not hits:
                        raise SearchFailure(
                            "search_output_limit",
                            "Search response budget cannot fit one excerpt",
                        )
                    limited = True
                    break
                hits, last = candidate, next_cursor
            if not limited:
                code = await process.wait()
                stderr = await errors
                if code not in (0, 1) or (code == 1 and stderr.strip()):
                    raise SearchFailure(
                        "search_failed", stderr[:1000].decode(errors="replace")
                    )
        outcome = _outcome(hits, last if limited else None)
        if _size(outcome) > max_bytes:
            raise SearchFailure(
                "search_output_limit", "Search response exceeds its byte limit"
            )
        return outcome
    except TimeoutError:
        return _failure(
            "timeout", "Search timed out; results are incomplete", max_bytes
        )
    except (asyncio.LimitOverrunError, asyncio.IncompleteReadError):
        return _failure(
            "search_output_limit",
            "Search match exceeds the bounded transport; narrow the query",
            max_bytes,
        )
    except SearchFailure as error:
        return _failure(error.code, str(error), max_bytes)
    except OSError:
        return _failure(
            "search_failed",
            "Cannot safely read search results; search again",
            max_bytes,
        )
    finally:
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        # Drain a paused pipe while reaping: waiting first can deadlock after overflow.
        cleanup = asyncio.gather(
            process.wait(), _discard(process.stdout), errors, return_exceptions=True
        )
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await cleanup
            raise

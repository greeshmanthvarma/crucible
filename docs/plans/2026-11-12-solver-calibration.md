# Solver Calibration Implementation Plan

**Goal:** Let a Crucible coding trial reach a valid solution patch within its existing 200k-token budget by cutting resent context and making edits succeed, before any further self-improvement run.

**Architecture:** Four hand-made harness changes plus a free offline replay. The listing tool returns a bounded breadth-first overview. A new protected projection shortens older read-only tool results in coding-trial requests, leaving the stored trace unchanged. A new `edit_file` tool provides exact search-and-replace edits, and `apply_patch` arguments are normalized (hunk recount, stray wrapper lines) before the protected parser admits them. The dispatcher admits mutations from the adapted arguments that the tool will actually execute. A replay script measures the effect against the recorded `full-run-v1` traces.

**Tech stack:** Python 3.13, pydantic, pytest (asyncio), ruff, mypy, `uv`.

**Spec:** Accepted recommendation in the Nov 12, 2026 review of `docs/self-improvement-handoff.md`, section 4 ("Solver calibration"), plus the trace analysis below.

### Evidence this plan targets (full-run-v1, 105 graded trials)

- Median 9 model requests per trial; the 200k cumulative cap ends trials long before the 20-step cap.
- Every trial starts with `list_files`; its result averages ~31k characters and is resent on every later request. Estimated median share of input tokens: ~51%.
- Compaction triggers only above ~76k tokens per request; requests average ~19k, so compaction never runs.
- 229/250 `apply_patch` calls failed; ~205 at Crucible's own parser (wrong hunk counts, `*** End Patch` / `*** Update File:` lines, bare `@@`), ~15 at `git apply`.

## Global constraints

- Protected authority stays protected: `patch_paths`, `WorkspaceAuthority`, `admit_mutation` path confinement and symlink checks keep their current semantics. New mutation paths are admitted only through `admit_mutation`.
- Stored conversation, tool calls (raw arguments) and tool results are never rewritten. Projection affects only the outgoing request.
- No change to trial limits, the manifest schema, budgets, grading, or the improvement controller.
- Editable-surface files (`tools/filesystem.py`, `tools/git_tools.py`, `tools/behavior_contracts.py`, `context/selection.py`, `context/summary_behavior.py`, `engine/coding_behavior.py`) may change by hand here; `EDITABLE_PATHS` itself does not change.
- `make check`-equivalent stays green: `uv run ruff format --check`, `uv run ruff check`, `uv run mypy`, `uv run pytest -q`.

## Review focus

1. **Projection must not drop recent or mutation evidence.** Last two tool batches and all `apply_patch`/`write_file`/`edit_file` results stay verbatim — `test_projection_keeps_recent_batches_and_mutation_results` (Task 2).
2. **Normalization must not widen authority.** Admission parses the adapted patch that is applied; an adapted patch targeting a symlink or outside path is still rejected — `test_dispatcher_admits_adapted_patch_paths` and `test_adapted_patch_cannot_escape_workspace` (Task 4).
3. **Hunk recount must not misread removed `-- ` lines as headers** — `test_recount_keeps_removed_double_dash_line` (Task 4).
4. **`edit_file` must fail closed on ambiguity and absence** with no partial write — `test_edit_file_rejects_missing_and_ambiguous_text` (Task 3).
5. **Truncated listing must say how to continue** and stay deterministic — `test_list_files_is_breadth_first_and_explains_truncation` (Task 1).

## File map

| File | Responsibility | Status |
| --- | --- | --- |
| `backend/src/crucible/tools/filesystem.py` | Bounded breadth-first `ListFilesTool` | Modify (editable) |
| `backend/src/crucible/context/result_projection.py` | Protected coding-trial tool-result projection | Create |
| `backend/src/crucible/application/worker_container.py` | Wire projection for non-role (coding) worker jobs | Modify |
| `backend/src/crucible/tools/definitions.py` | `EditFileArguments` | Modify |
| `backend/src/crucible/tools/execution_policy.py` | Register `edit_file` as a mutation; admission | Modify |
| `backend/src/crucible/tools/git_tools.py` | `EditFileTool` | Modify (editable) |
| `backend/src/crucible/tools/registry.py` | Add `EditFileTool` to `default_registry` | Modify |
| `backend/src/crucible/tools/behavior_contracts.py` | `edit_file` description, `apply_patch` normalization in `adapt_arguments` | Modify (editable) |
| `backend/src/crucible/tools/dispatcher.py` | Admit mutations from adapted arguments | Modify |
| `backend/src/crucible/improvement/case_grouping.py` | `edit_file_failure` label | Modify |
| `backend/src/crucible/improvement/case_evidence.py` | Withhold `edit_file` call payloads | Modify |
| `backend/tests/fixtures/coding_behavior_*.json` | Request snapshots (tool list/descriptions) | Regenerate |
| `backend/scripts/replay_solver_calibration.py` | Offline replay report | Create |

Dependency direction: `context/result_projection.py` depends only on `crucible.engine.gateway` and `crucible.context.invariants`; `worker_container` imports it. Tools depend on `definitions`/`execution_policy` as today.

## Tasks

### Task 1: Bounded breadth-first `list_files`

**Files:**
- Modify: `backend/src/crucible/tools/filesystem.py`
- Test: `backend/tests/integration/tools/test_read_tools.py`

**Interfaces:**
- Produces: `ListFilesTool(max_entries: int = 250, max_bytes: int = 8_000)`. Data `{"paths": list[str], "truncated": bool}`; directories appear as `"name/"`. Display text is the newline-joined paths, plus, when truncated, a final line `LIST_TRUNCATED_NOTICE`.

- [x] **Step 1: Write the failing test**

```python
async def test_list_files_is_breadth_first_and_explains_truncation(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    (workspace / "pkg" / "deep").mkdir(parents=True)
    (workspace / "README.md").write_text("x")
    (workspace / "pkg" / "mod.py").write_text("x")
    (workspace / "pkg" / "deep" / "leaf.py").write_text("x")
    context = ToolContext(workspace)

    full = await ListFilesTool().invoke(context, {})
    cut = await ListFilesTool(max_entries=3).invoke(context, {})

    assert full.data["paths"] == [
        "README.md", "pkg/", "pkg/deep/", "pkg/mod.py", "pkg/deep/leaf.py"
    ]
    assert full.truncated is False
    assert cut.data["paths"] == ["README.md", "pkg/", "pkg/deep/"]
    assert cut.truncated is True
    assert cut.display_text.endswith(LIST_TRUNCATED_NOTICE)
```

Existing `test_read_tools_are_bounded_stable_and_workspace_relative` must keep passing (`["a.txt"]` with `max_entries=1`).

- [x] **Step 2: Verify the red state**

Run: `cd backend && uv run pytest -q tests/integration/tools/test_read_tools.py`
Expected: FAIL — `LIST_TRUNCATED_NOTICE` import error / depth-first, files-only ordering.

- [x] **Step 3: Implement**

Breadth-first walk with a `collections.deque` of directories, starting at the resolved `path`. Per directory, entries sorted by name; skip `.git` and `.crucible`; do not follow symlinked directories (list them as files). Directories are emitted as `relative + "/"` and enqueued. Stop at `max_entries` or when the next entry would exceed `max_bytes` (UTF-8 bytes of paths). Keep BFS order (no final sort).

```python
LIST_TRUNCATED_NOTICE = (
    "[Listing truncated. Deeper paths were omitted. Call list_files with a "
    "subdirectory path, or use search_files to locate code.]"
)
```

Update the `list_files` description: `"List workspace-relative paths breadth-first; directories end with /. Large trees are truncated; pass a subdirectory path to see deeper levels."`

- [x] **Step 4: Verify the green state** — same command, PASS.
- [x] **Step 5: Broader checks** — `uv run pytest -q tests/integration/tools tests/unit/tools`.

### Task 2: Coding-trial tool-result projection

**Files:**
- Create: `backend/src/crucible/context/result_projection.py`
- Modify: `backend/src/crucible/application/worker_container.py`
- Test: `backend/tests/unit/context/test_result_projection.py`

**Interfaces:**
- Produces:
  ```python
  KEEP_RECENT_BATCHES = 2
  SHORTEN_ABOVE_CHARS = 1_200
  PREVIEW_CHARS = 400
  SHORTENED_NOTICE = "\n[Earlier output shortened to save context. Repeat a narrower call if needed.]"
  PROJECTED_TOOLS = frozenset({"list_files", "read_file", "search_files",
                               "workspace_diff", "workspace_status", "execute_command"})

  def project_coding_tool_results(
      messages: tuple[ModelMessage, ...],
      observation: BehaviorObservation,
      estimated_input_tokens: int,
      max_output_tokens: int,
      estimator: TokenEstimator,
  ) -> tuple[ModelMessage, ...]
  ```
  Signature matches `ContextManager(project_tool_results=...)`. Unconditional: the cumulative budget, not a single request, is the binding limit.

- [x] **Step 1: Write the failing tests**

```python
def test_projection_shortens_old_read_results():
    # four batches: list_files (5000 chars), read_file, read_file, read_file
    projected = project_coding_tool_results(messages, observation(), 0, 0, SimpleTokenEstimator())
    first = result_text(projected, "call-1")
    assert first == "x" * PREVIEW_CHARS + SHORTENED_NOTICE

def test_projection_keeps_recent_batches_and_mutation_results():
    # old apply_patch result (2000 chars) and the last two read batches stay verbatim
    ...

def test_projection_leaves_short_results_and_preserves_structure():
    # results <= SHORTEN_ABOVE_CHARS unchanged; message count, roles, tool_call_ids identical
    ...
```

Helpers build `ModelMessage(ModelRole.ASSISTANT, (ModelPart("tool_call", None, tool_call_id=..., tool_name=..., arguments={}),))` followed by `ModelMessage(ModelRole.TOOL, (ModelPart("tool_result", text, tool_call_id=..., tool_name=...),))`.

- [x] **Step 2: Verify red** — `uv run pytest -q tests/unit/context/test_result_projection.py` → `ModuleNotFoundError`.
- [x] **Step 3: Implement**

Batches = assistant messages containing `tool_call` parts, in order. Old call ids = calls in all but the last `KEEP_RECENT_BATCHES` batches. Replace `text_content` of `tool_result` parts whose `tool_call_id` is old, `tool_name in PROJECTED_TOOLS`, and length `> SHORTEN_ABOVE_CHARS` with `text[:PREVIEW_CHARS] + SHORTENED_NOTICE`. Everything else identical (use `dataclasses.replace`). `ContextManager` already keeps the projection only when its estimate is lower.

Wire in `create_worker_engine`:
```python
project_tool_results=(
    prune_proposal_tool_results if is_proposer
    else project_coding_tool_results if not isinstance(job, RoleJob)
    else None
),
```

- [x] **Step 4: Verify green** — same command PASS.
- [x] **Step 5: Broader** — `uv run pytest -q tests/integration/context tests/contract/test_improvement_worker.py tests/integration/test_improvement_worker_trace.py`.

### Task 3: `edit_file` exact search-and-replace tool

**Files:**
- Modify: `tools/definitions.py`, `tools/execution_policy.py`, `tools/git_tools.py`, `tools/registry.py`, `tools/behavior_contracts.py`, `improvement/case_grouping.py`, `improvement/case_evidence.py`
- Regenerate: `tests/fixtures/coding_behavior_baseline.json`, `tests/fixtures/coding_behavior_recovery.json`
- Test: `backend/tests/integration/tools/test_mutation_tools.py`, `backend/tests/integration/tools/test_dispatcher.py`

**Interfaces:**
- Produces:
  ```python
  class EditFileArguments(ToolArguments):
      path: str
      old_text: str = Field(min_length=1, max_length=200_000)
      new_text: str = Field(max_length=1_000_000)
      replace_all: bool = False
  ```
  `EditFileTool` (registered identity `"edit_file"`, `parallel_safe = False`, mutation). Outcomes:
  - success: data `{"path", "replacements": int}`, display `"Replaced {n} occurrence(s) in {path}"`.
  - `error_code="edit_no_match"`: display names the path and says to re-read and copy exact lines including indentation.
  - `error_code="edit_ambiguous"`: display gives the match count and says to add surrounding lines or set `replace_all`.
  - `error_code="file_not_found"`: display says to use `write_file` to create files.
  - `old_text == new_text`: `error_code="edit_noop"`.

- [x] **Step 1: Write the failing tests**

```python
async def test_edit_file_replaces_unique_text(tmp_path): ...
async def test_edit_file_rejects_missing_and_ambiguous_text(tmp_path):
    # file "a\na\n": old_text "a" -> edit_ambiguous; "zzz" -> edit_no_match; file unchanged
    ...
async def test_edit_file_replace_all(tmp_path): ...
async def test_dispatcher_edit_file_is_admitted_and_confined(tmp_path):
    # via default_registry(): edit of "../x" or a symlink target is rejected (tool_exception)
    ...
```

- [x] **Step 2: Verify red** — `uv run pytest -q tests/integration/tools/test_mutation_tools.py tests/integration/tools/test_dispatcher.py` → import errors.
- [x] **Step 3: Implement**

`admit_mutation`: `elif name == "edit_file": paths = (normalize_path(EditFileArguments.model_validate(arguments).path),)`. Add `"edit_file"` to `_MODELS`, `registered`, the `mutation` set and the non-parallel set. `EditFileTool.invoke` reads via `authority.read_text`, counts `content.count(old_text)`, writes once via `authority.write_text`. Description:

> Replace exact text in one workspace file. Copy old_text exactly from read_file output, including indentation, with enough surrounding lines to match once; set replace_all to change every match. Prefer this for targeted edits; use write_file to create files.

Prefix the `apply_patch` description with `"Prefer edit_file for targeted changes. "`. Add `"edit_file": "edit_file_failure"` to `_TOOL_LABELS` and `("tool_call", "edit_file")` to `_WITHHELD`. Regenerate both snapshot fixtures; the diff must contain only the new tool definition and changed descriptions.

- [x] **Step 4: Verify green** — same command PASS.
- [x] **Step 5: Broader** — `uv run pytest -q tests/unit/test_coding_behavior_boundary.py tests/unit/test_case_grouping.py tests/unit/test_case_evidence.py tests/integration/tools`.

### Task 4: Normalize `apply_patch` arguments and admit what is applied

**Files:**
- Modify: `tools/behavior_contracts.py`, `tools/dispatcher.py`
- Test: `backend/tests/unit/tools/test_patch_normalization.py`, `backend/tests/integration/tools/test_dispatcher.py`

**Interfaces:**
- Produces: `normalize_unified_diff(patch: str) -> str` in `behavior_contracts`. `adapt_arguments("apply_patch", args)` returns args with the normalized patch. The dispatcher calls `admit_mutation(name, item.arguments, workspace)` (adapted, re-validated arguments) instead of the raw `item.record.arguments`; the record keeps raw arguments.

Normalization rules, in order:
1. Drop lines exactly `*** Begin Patch` or `*** End Patch` (after `rstrip`).
2. `*** Update File: P` becomes `--- a/P` and `+++ b/P`.
3. A `--- ` line is a file header only when the next line starts with `+++ `; otherwise it is a hunk body line.
4. For each `@@ -a[,b] +c[,d] @@tail` header, collect body lines until the next header (`@@ `, a file-header pair, `diff --git `) or end. Inside the body, an empty line counts as a blank context line (`" "`); trailing empty lines at the end of a body are dropped. If any body line starts with something other than `" "`, `"+"`, `"-"`, `"\\"`, return the original patch unchanged. Rewrite counts as `old = context + removed`, `new = context + added`.
5. Ensure a trailing newline.
Bare `@@` headers are left alone (the protected parser rejects them).

- [x] **Step 1: Write the failing tests**

```python
def test_recount_fixes_wrong_hunk_counts(): ...
def test_strips_wrapper_lines_and_converts_update_file_header(): ...
def test_recount_keeps_removed_double_dash_line():
    patch = "--- a/x.sql\n+++ b/x.sql\n@@ -1,1 +1,1 @@\n--- old comment\n keep\n"
    assert normalize_unified_diff(patch) == patch.replace("-1,1 +1,1", "-1,2 +1,1")
def test_unknown_body_line_returns_original(): ...
async def test_dispatcher_admits_adapted_patch_paths(tmp_path):
    # wrong counts + "*** End Patch" applies through the dispatcher and changes file.txt
async def test_adapted_patch_cannot_escape_workspace(tmp_path):
    # normalized patch targeting a symlinked path is still rejected; no file changes
```

- [x] **Step 2: Verify red** — `uv run pytest -q tests/unit/tools/test_patch_normalization.py tests/integration/tools/test_dispatcher.py` → import error / malformed-hunk failure.
- [x] **Step 3: Implement** as specified above.
- [x] **Step 4: Verify green** — same command PASS.
- [x] **Step 5: Broader** — `uv run pytest -q tests/integration/tools tests/integration/repair tests/contract/test_improvement_roles.py`.

### Task 5: Offline replay against full-run-v1

**Files:**
- Create: `backend/scripts/replay_solver_calibration.py`

**Interfaces:**
- Consumes: Tasks 1–4. CLI: `uv run python scripts/replay_solver_calibration.py <trials-dir>`; prints JSON.
- Reports:
  - `patches`: failed `apply_patch` calls; how many now pass `patch_paths(normalize_unified_diff(p))`; for those, how many pass `git apply --check` against a clean checkout of the trial's base commit (only where the trial workspace is a Git repository with recoverable `HEAD`; otherwise counted as `unchecked`).
  - `listing`: old vs new `list_files` display size on each case's workspace.
  - `tokens`: per trial, the recorded trajectory's request-by-request input estimate (`SimpleTokenEstimator`) with the old opening listing and no projection, versus the new listing size and `project_coding_tool_results`; median ratio and the number of requests the same trajectory would fit in 200k.

This replay holds the model's behavior fixed, so it measures harness cost only. It cannot show pass-rate changes.

- [x] **Step 1:** Write the script; run it; record the output in the results section below.
- [x] **Step 2:** Full checks: `uv run ruff format --check src tests scripts && uv run ruff check --no-cache src tests scripts && uv run mypy && uv run pytest -q`.

## Deliberate exclusions

- **Budget semantics.** The cap is checked after each response, so a final response over budget is discarded with any edit it contained. Changing this alters protected accounting and ledger reservations; decide separately with the paid calibration.
- **Native `*** Begin Patch` (V4A) support.** Luna appears trained on it, but bare `@@` hunks need file content to place, and admission would need a protected V4A path parser. Revisit only if `edit_file` adoption is low.
- **Paid calibration and an unchanged-harness repeat.** Requires operator approval and budget (see handoff below).
- **Miner taxonomy and editable-surface changes.** The next plan, after calibration.
- **Proposer projection refactor.** `proposal_context._shorten_outputs` stays as is.

## Execution handoff

Implement Tasks 1–5 in order, one commit per task. Then the operator runs a paid isolated calibration on the five development cases with the new harness and matched limits (`benchmark isolated-trial`, three repeats each), plus one unchanged-harness repeat at the old commit to estimate noise.

## Results

Offline replay completed on all 105 recorded traces using:

```sh
cd backend
uv run python scripts/replay_solver_calibration.py ../data/self-improvement/full-run-v1/runtime/trials
```

```json
{
  "trials": 105,
  "patches": {
    "failed": 229,
    "invalid_arguments": 0,
    "raw_parseable": 23,
    "normalized_parseable": 223,
    "recovered_by_normalization": 200,
    "still_rejected": 6,
    "git_apply_pass": 140,
    "git_apply_fail": 83,
    "unchecked": 0
  },
  "listing": {
    "mean_before_chars": 33642.114285714284,
    "mean_after_chars": 5858
  },
  "tokens": {
    "median_ratio": 0.258549876785607,
    "median_total_before": 144714,
    "median_total_after": 38865,
    "median_requests_that_fit_before": 9,
    "median_requests_that_fit_after": 9,
    "requests_that_fit_before": 1047,
    "requests_that_fit_after": 1047
  }
}
```

Normalization newly recovers 200 of 229 failed calls: 23 already passed the
protected parser, 223 pass after normalization, and six remain rejected. Of the
223 normalized candidates, 140 pass `git apply --check` against an independent
clean checkout; 83 fail and none are unchecked. Each check starts from the
workspace's synthetic `Solver base` HEAD (the recoverable base snapshot, not the
upstream SWE-bench commit ID, which is absent from these repositories). No prior
trial edits are replayed into that checkout.

Mean opening listing size falls from 33,642.11 to 5,858 characters (82.6% lower).
The median per-trial input estimate ratio is 0.25855 (74.1% lower); median totals
are 144,714 before and 38,865 after. These are `SimpleTokenEstimator` text-only
input estimates, including harness and root instructions; they exclude tool
schemas, argument payloads and output tokens, so they are not provider billing
or full cumulative model-budget accounting.

The median number of observed requests fitting in 200k is 9 before and 9 after
(1,047 requests total in either condition). This metric is capped at the recorded
trajectory length: the old text-only estimates already fit every observed
trajectory. It does not extrapolate extra model turns or demonstrate a pass-rate
change. Per-trial request estimates and listing sizes grouped by case are printed
in the script's JSON output.

Implementation deviations:

- Updated the existing adapter-boundary test to reject an adapted symlink target.
  Its previous assertion required admission of raw paths, which directly
  conflicted with Task 4. Protected authority implementation is unchanged.
- Added `tests/unit/context/__init__.py` because the specified
  `test_result_projection.py` filename collides with the existing integration
  eval test under pytest's default import mode. The initial full run exposed
  this collection error; the package marker resolves it.

Verification completed:

- Task 1: narrow read-tool suite 2 passed; broader tool suites 43 passed.
- Task 2: projection suite 3 passed; broader context/worker suites 45 passed.
- Task 3: mutation/dispatcher suite 12 passed; broader behavior/grouping/evidence/tool
  suites 82 passed. Both fixture diffs were confirmed to contain only the new
  tool definition and changed descriptions.
- Task 4: normalization/dispatcher suite 13 passed; with the adjusted authority
  boundary suite, 26 passed. The initial broader run had 264 passed and the one
  obsolete raw-admission assertion described above; all 47 tool tests and eight
  role-contract tests then passed on the updated code. The final full run also
  covers the complete broader repair selection and passes.
- Task 5: offline synthetic checks confirmed independent clean-base patch checks,
  preservation of a dirty source workspace, request-prefix accounting, and the
  exact 200k input-budget boundary.
- Final required check: Ruff format check (402 files), Ruff lint, and mypy
  (208 source files) passed. Full pytest: **1,325 passed, 22 skipped, 1 warning**
  in 1,712.17 seconds. The warning comes from Alembic reflecting an unnamed CHECK
  constraint in the unchanged task-provisioning migration test. No unresolved
  or pre-existing test failures were observed.

The initial full run exposed the test-module name collision described above;
that introduced collection failure was fixed before the successful full rerun.
No paid model calls, Docker benchmark trials, or isolated-trial runs were used.
The user's separate changes in `improvement/store.py` and
`tests/integration/test_improvement_final.py` remain unmodified and uncommitted.

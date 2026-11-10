# Case evidence assembler offline verification

## Checks

Run from the repository root. Task 1 red failed collection on the missing
assembler module, then the targeted suite passed 5 tests. Task 2 red had 4
expected failures for the missing expansion function and CLI operation, then
the targeted suite passed all 9 tests.

```sh
cd backend
export CRUCIBLE_MODEL=fake LITELLM_LOCAL_MODEL_COST_MAP=true
.venv/bin/pytest -q tests/unit/test_case_evidence.py
.venv/bin/pytest -q tests/unit/test_case_evidence.py tests/contract/test_case_evidence_preview.py
.venv/bin/pytest -q tests/unit/test_case_evidence.py tests/unit/test_trajectory.py tests/unit/test_trajectory_export.py tests/unit/context/test_local_tokens.py
.venv/bin/pytest -q tests/unit/test_case_evidence.py tests/unit/test_trajectory.py tests/unit/test_trajectory_export.py tests/unit/context/test_local_tokens.py tests/contract/test_case_evidence_preview.py tests/integration/test_trajectory_archive.py
.venv/bin/ruff check src tests
.venv/bin/ruff format --check src tests
.venv/bin/mypy src/crucible
cd ..
git diff --check
```

The Task 1 broader suite passed 45 tests. The Task 2 broader suite passed 77.
Ruff lint, Ruff format, mypy, and Git whitespace checks passed. Token counts
used the installed, integrity-checked local vocabulary. The CLI contract test
blocks the experiment store, provider, and Docker constructors, verifies both
previews and stable errors without a state root, and checks package hashes.

## Saved pytest development repeat

These commands were run from the repository root, with JSON directed to a
private temporary directory. Only counts and selected metadata are recorded here.

```sh
umask 077
proof=$(mktemp -d /private/tmp/crucible-case-evidence.XXXXXX)
package="$PWD/data/self-improvement/trajectory-export-v1/saved-pilot-v9-final"
pin=dae35f2e7293c18951c9d3c483101278335fb6ff046daca7980ecd552fa6e149
trial=a70c95af-47af-4670-a251-39cdf82b056d
find "$package" -type f -exec shasum -a 256 {} + | LC_ALL=C sort > "$proof/before.sha256"
CRUCIBLE_MODEL=fake LITELLM_LOCAL_MODEL_COST_MAP=true backend/.venv/bin/crucible improve evidence-preview \
  --package "$package" --bundle-digest "$pin" --trial "$trial" > "$proof/initial.json"
CRUCIBLE_MODEL=fake LITELLM_LOCAL_MODEL_COST_MAP=true backend/.venv/bin/crucible improve evidence-preview \
  --package "$package" --bundle-digest "$pin" --trial "$trial" \
  --detail E33 --detail E31 > "$proof/expanded.json"
find "$package" -type f -exec shasum -a 256 {} + | LC_ALL=C sort > "$proof/after.sha256"
cmp "$proof/before.sha256" "$proof/after.sha256"
wc -l "$proof/before.sha256"
wc -c "$proof/initial.json" "$proof/expanded.json"
```

- All 340 file hashes matched before and after. The canonical packet was 11,233
  bytes and identical in both previews. CLI output measured 25,470 bytes for
  the initial preview and 40,415 bytes with the follow-up, including newlines.
- The initial request used 4,502 estimated input tokens and a 2,000-token
  output reserve, for 6,502 reserved tokens. The follow-up used 4,698 estimated
  input tokens including the exact initial messages, plus a second 2,000-token
  output reserve. The two requests together reserved 13,200 tokens. The
  canonical message arrays measured 14,107 and 14,886 bytes respectively.
- The packet displayed 42 rows from the 77-record index and advertised 33
  detail IDs. It excluded conversation scaffolding and represented the task
  instruction separately. The budget omitted no event ranges and kept the
  comparator. Task text fit its bound. Nine source-bearing detail IDs were
  withheld and 12 IDs were marked truncated, including projected patch calls.
  The package recorded no missing results or unresolved part references.
- E33 linked to its call E31. The result supplied a 68-character sanitized
  excerpt. The call supplied a 42-character metadata projection flagged as
  truncated. Both fit at the original follow-up excerpt limit.
- The failed repeat showed 11 failed patch results, no successful patch result,
  and an empty submission. The selected passing repeat showed seven failed
  patch results, one successful patch result, and a nonempty submission with
  complete paths. Both solvers ended on budget. The contrast remained legible.
  The comparator has aggregate outcomes, so they establish no ordering or
  causal claim about recovery.

## Disclosure and coverage limits

The exporter can seal sanitized fields that still contain source text. The
preview withholds direct read, search, and workspace diff results, write
payloads, and command arguments. It projects patch calls to size and parsed
paths, and command results to recorded exit code and termination when
available. These projections are marked incomplete, including in follow-ups.
Assistant text excerpts exclude structural and reasoning parts. Free-form
task, assistant, and eligible tool text still relies on the trusted development
export and its sanitization. The seal verifies integrity and the caller's pin
establishes provenance. Keep the preview private.

The synthetic long trace exercised row reduction at 2,200 initial input tokens.
Its omitted ranges were 14–24, 27–48, 51–72, and 75–96. All 24 failed and 24
successful patch outcomes remained in the counts. The separate synthetic
nonpatch fixture retained a failed check and its exit code alongside a later
unrelated successful listing and an empty submission. Its raw command output
was withheld, and the response contract preserved uncertainty.

Pilot v9 provides one saved development case. Real cross-case verification is
pending another approved saved development case. The proof used synthetic
fixtures and that one saved package. It establishes request fit and evidence
visibility. Diagnosis quality remains unmeasured.

# Crucible

Crucible is a local coding-agent harness for repository-level work: the model edits an
isolated Git worktree, every command runs in a digest-bound Docker sandbox with
human approval, and the harness—not the model—owns validation, acceptance, and
integration.

It is built as an eval-driven system: ordinary Tasks and Runs produce durable
traces, and a Modest Eval Runner scores pinned fixtures offline. Bounded repair runs can diagnose supported failures, verify candidate fixes in
isolated workers, and publish one PR for manual review.

## Why it exists

Most “agent that edits your repo” demos blur authority. Crucible keeps hard edges:

- **Isolated workspaces** — Tasks use linked Git worktrees; the registered checkout
  stays clean until you explicitly integrate.
- **Human-gated commands** — `execute_command` never runs a shell string on the
  host. Each call is an Approval bound to the executable, args, and SHA-256 digest
  shown in the UI.
- **Harness-authored validation** — configured Validation commands decide success;
  model completion claims do not.
- **Durable conversation and evidence** — Messages, Context Manifests, Artifacts,
  Result Revisions, and SSE Events survive refresh and restart.
- **Offline evals** — pinned fixture commits, deterministic fake-model trajectories,
  and hidden Docker checks produce retained Trial reports without writing into the
  coding Task workspace.

## Architecture

```text
Register Repository
        │
        ▼
   Create Task ──► Workspace (linked worktree)
        │
        ▼
   Conversation ◄── Steering / Compaction
        │
        ▼
      Run ──► Model loop (Steps + Tool Calls)
                │
                ├─ repo tools (read/search/write/patch/diff)
                └─ execute_command ──► Approval ──► Docker sandbox
        │
        ▼
   Validation (bounded repair) ──► Outcome
        │
        ▼
   Acceptance ──► immutable Result Revision
        │
        ▼
   Integration ──► target checkout (only when proven safe)
```

Stack: FastAPI + SQLite + Artifact Store on the backend; React + Vite on the
frontend; LiteLLM as a provider-neutral model gateway; Docker for command
isolation. Domain and Run Engine stay independent of FastAPI, LiteLLM, and React.

## Quick start

Requirements: Git, Python 3.13, [uv](https://docs.astral.sh/uv/), Node.js 24, pnpm.
Docker is required for real sandbox execution and eval behavioral checks; the
default quality gate runs offline without a live model.

```sh
make install
make check
```

`make check` runs lint, types, tests, migration round-trip, OpenAPI drift check,
and the production frontend build. Optional smokes:

| Target | Purpose |
| --- | --- |
| `make milestone` | Deterministic end-to-end milestone workflow proof |
| `make smoke-eval` | One Trial per smoke Eval Case (deterministic gateway) |
| `make smoke-docker` | Real Docker lifecycle (needs daemon + image) |
| `make smoke-real-model` | Live provider connectivity (opt-in credentials) |

### Local demonstration

Create a disposable committed repository so Crucible never touches an existing
checkout:

```sh
mkdir -p /tmp/crucible-demo-repository
git -C /tmp/crucible-demo-repository init
git -C /tmp/crucible-demo-repository config user.email demo@example.com
git -C /tmp/crucible-demo-repository config user.name "Crucible Demo"
printf 'original\n' > /tmp/crucible-demo-repository/example.txt
git -C /tmp/crucible-demo-repository add example.txt
git -C /tmp/crucible-demo-repository commit -m fixture
```

In separate terminals:

```sh
make dev-backend
make dev-frontend
```

The backend prints a one-time bootstrap secret at startup. Open
`http://127.0.0.1:8000/#bootstrap=<secret>` once to exchange it for an opaque,
revocable `HttpOnly; SameSite=Strict` session cookie. For the Vite app, set
`CRUCIBLE_DEVELOPMENT_ORIGIN=http://127.0.0.1:5173` so credentialed CORS matches
that exact origin.

Then open the URL printed by Vite, register `/tmp/crucible-demo-repository`,
create a Task from `HEAD`, and send a Message. The UI shows the Run lifecycle and
reconstructs the same Task and Conversation from SQLite after refresh. Confirm the
registered checkout stayed clean:

```sh
git -C /tmp/crucible-demo-repository status --porcelain=v1
```

## What this milestone includes

- Durable Conversations, steering, proactive Compaction
- Completion proposals, authoritative Validation, bounded repair
- Immutable Result Revisions, Acceptance, guarded Integration
- Provider-neutral model gateway with Context Manifests and step/tool budgets
- Digest-bound Docker Approvals with private Artifacts
- Modest Eval Runner for local Suites, Trials, and report Artifacts

Deliberately not in this slice: self-improvement campaigns, promotion/rollback,
multi-user hosting, remote workers, PR creation, and automatic interactive
allowlists. See [`docs/deferred-capabilities.md`](docs/deferred-capabilities.md).

Domain language lives in [`docs/CONTEXT.md`](docs/CONTEXT.md).

## Modest Eval Runner

The Eval Runner is an explicit local CLI. It creates ordinary Tasks and Runs from
pinned Git fixture commits, evaluates the Workspace after each Run, and retains
each Trial, report Artifact, and raw trace reference. Hidden behavioral checks
need a reachable Docker daemon and the pinned Alpine evaluator image under
`evals/development/cases/`. The `smoke` Suite has one harness Case and two small
coding Cases; hidden test source never enters the coding Task Workspace.

From `backend/`, run a repeatable offline proof in a fresh data directory:

```sh
uv run crucible eval run smoke --deterministic --trials 2 --data-dir "$(mktemp -d /tmp/crucible-eval.XXXXXX)"
```

`make smoke-eval` runs one Trial per Case. The deterministic gateway uses the
normal model/tool loop with a versioned local trajectory (`fake` model, no
provider credentials). A live model run is opt-in: set a real model in a Case
manifest and invoke `eval run` without `--deterministic`.

```sh
uv run crucible eval show <invocation-id> --data-dir <same-dir>
```

Each row links to a private JSON report (fixture/config digests, verdict,
evaluator evidence, metrics, raw Run trace IDs). `--trials N` repeats each Case
with a fresh fixture and separate Task/Run identities. `estimated_cost` stays
`null` until you pass a versioned `--price-table`.

Case manifests may set `[budgets]`, `[settings]` (including Validation command
specs), and `[setup].required_paths`. If a Case requests `execute_command`, the
CLI prints the exact command and Approval digest and waits for `approve` or
`deny`—closing input does not imply approval. Held-out partitions need
`--partition held-out` and `--held-out-root <private-root>`; deterministic
trajectories are unavailable there.

## Backend

Requires CPython 3.13 and uv. From `backend/`:

```sh
uv sync
uv run pytest
uv run ruff check .
uv run ruff format .
uv run mypy src/crucible
uv run alembic upgrade head
uv run uvicorn crucible.api.app:create_app --factory --host 127.0.0.1 --reload
```

Production defaults to one origin (`http://127.0.0.1:8000`). Set
`CRUCIBLE_ORIGIN` when changing the API port, `CRUCIBLE_DEVELOPMENT_ORIGIN` for
the Vite origin, and `CRUCIBLE_SECURE_COOKIE=true` for HTTPS. Unsafe API requests
require that exact Origin plus a CSRF token. The session-refresh endpoint can
recover a CSRF token from a valid session cookie after a reload, but still
requires the exact Origin.

### Milestone-one workflow

Repository registration stores the canonical Git root. Repository settings
snapshot Validation commands, sandbox image/network policy, repair bounds, and
Compaction limits into each Run.

A Task owns one retained Conversation across Runs. Steering joins the active Run
at the next model boundary; a message after a terminal Run starts another. 
Compaction summarizes complete semantic units without deleting original Messages,
traces, or Artifacts. Validation is harness-authored; repair stays bounded inside
the same Run.

Acceptance creates an immutable Result Revision in the Task Workspace and leaves
the registered checkout unchanged. Integration needs a selected Result Revision,
the expected target ref/revision, and a clean target. Conflicts abort only when
exact restoration is proven; otherwise durable state is `recovery_required`.
Integrated Tasks keep Conversation, Validation evidence, Result Revisions,
Artifacts, and Integration history for later review.

## Frontend

Requires Node.js 24 and pnpm. From `frontend/`:

```sh
pnpm install
pnpm test --run
pnpm lint
pnpm typecheck
pnpm build
pnpm dev
```

## Model configuration

Crucible embeds LiteLLM `1.102.0` behind a single Responses gateway for all real
providers. Provider/model pairs must support LiteLLM's Responses interface;
unsupported combinations fail without a Chat Completions fallback. The `fake`
model remains available for deterministic local execution and tests. Set the
credential env var for the selected provider (for example `OPENAI_API_KEY`).
Secrets are read by the provider SDK and never written to Events, Context
Manifests, or Task worktrees. Default quality gates use captured chunks and
scripted gateways only.

```sh
export CRUCIBLE_MODEL=openai/gpt-6-luna
export CRUCIBLE_MODEL_INPUT_LIMIT=100000
export CRUCIBLE_MODEL_OUTPUT_RESERVE=4096
export CRUCIBLE_MAX_STEPS=20
export CRUCIBLE_MAX_TOOL_CALLS=100
export CRUCIBLE_MAX_MODEL_TOKENS=200000
export CRUCIBLE_MAX_ACTIVE_SECONDS=600
```

Defaults: 20 Steps per Run, at most 100 Tool Calls in one batch. Each request
records a Context Manifest (model, estimated input size, root instruction digest,
Tool schema digest). Oversize required evidence fails with `context_limit`;
exhausted Steps fail with `budget_exhausted`.

The Responses adapter uses stateless requests (`store=false`) and replays
Crucible's durable normalized conversation, including provider Tool Call IDs.
Encrypted reasoning-state replay is not yet persisted.

Repository tools: `list_files`, `search_files`, `read_file`, `write_file`,
`apply_patch`, `workspace_status`, `workspace_diff`. Paths stay inside the Task
worktree; reads/outputs are bounded; writes are atomic; a complete batch is
validated before any call begins. `execute_command` is the Docker-only command
boundary.

Opt-in live connectivity smoke:

```sh
export CRUCIBLE_REAL_MODEL=...
# plus the provider credential
uv run pytest tests/contract/models/test_real_model_smoke.py
```

## Approved Docker commands

Docker with a reachable daemon is required to provision Tasks and execute
commands. Each Task owns one labeled dependency volume at
`/workspace/node_modules`. Commands are structured executable + arg arrays—never
shell strings—and each receives a durable Approval bound to the Tool Call and the
digest shown in the UI. Denial terminalizes that call without cancelling the Run.

Approved containers run as `65532:65532` with a read-only root filesystem, all
capabilities dropped, `no-new-privileges`, bounded CPU/memory/PIDs/time/output,
and network `none` unless the displayed command requests outbound access. Only
the Task worktree and its verified dependency volume are mounted. Provider
credentials stay on the host gateway; only explicit non-secret env entries in the
digest-bound spec are supplied (names visible in the UI, values not).

Stdout/stderr is independently bounded for live Events, model-facing text, and
retained evidence. Private content-addressed Artifacts live under
`CRUCIBLE_DATA_DIR/artifacts` (default `backend/data/artifacts`) with mode-0600
files, retrieved by opaque Artifact IDs. Cancellation and restart reconciliation
stop/remove exact label-verified containers and never replay an uncertain
command. Orphaned or ownership-mismatched Docker resources are reported, not
automatically deleted.

Real lifecycle smoke (locally available, non-root-compatible image pinned by
digest):

```sh
export CRUCIBLE_DOCKER_TEST_IMAGE='example/runner@sha256:...'
cd backend
uv run pytest -m docker tests/integration/sandbox/test_docker_lifecycle.py -q
```

## Repair runs

In a chat with a failed Run, send `/repair`. The host diagnoses the incident,
reviews a sealed reproduction, verifies its baseline, and tries up to three
proposals per independently approved fix. Accepted fixes advance the next fix's
baseline. Fixed development and private validation gates must not regress.
The compact repair card survives refresh and shows the stage, charged role
attempts, safe outcome and PR link. Ordinary command approvals remain unchanged.

Use **Stop repair** to cancel active execution. Send `/repair resume` (or use
**Resume repair**) after stopping, an interruption or an infrastructure error;
startup never resumes automatically. `/repair close` closes an inactive repair
without deleting its evidence or worktree. Unknown edits stop for review and are
preserved. Stop before closing an active repair. The branch `repair/<repair-id>`
and `CRUCIBLE_DATA_DIR/repair-worktrees/<repair-id>` are retained.

Run the committed harness from a clean, current `main` checkout and restart it
before real repairs. Admission fetches remote main without changing working files
and checks the running source identity. Configure the Crucible origin on GitHub,
authenticate `gh` on the host, and keep Git author identity configured. One repair
executes globally; an existing open repair PR blocks another, including a repair
whose label application failed. Publication retries discover the saved head/base
PR before creating one. No accepted commits means no PR. Publication never
force-pushes or merges; merge the PR and update/restart main manually.

Repair workers require Docker and a locally available image pinned by digest:

```sh
export CRUCIBLE_REPAIR_IMAGE='your-worker@sha256:<full-image-digest>'
export CRUCIBLE_MODEL='your-provider/model'
# Set the selected provider's credential on the host, e.g. OPENAI_API_KEY.
export CRUCIBLE_REPAIR_GATEWAY_ENDPOINT='http://host.docker.internal:8000/internal/repair-model/responses'
```

The gateway endpoint must be reachable from workers at the backend's actual port.
Workers receive scoped, expiring gateway credentials and finite budgets; provider
and GitHub credentials remain on the host. Diagnosis/review cannot edit source;
proposal commands operate in the repair worktree, without private eval mounts.
Missing historical evidence can make a reproduction unsupported; it must not be
invented to obtain an improvement.

Accepted incidents are private local regression data. Back up the database,
`CRUCIBLE_DATA_DIR/artifacts`, `repair-incidents`, and `repair-fixtures` together;
references alone are not portable. Keep these private files out of Git and PR
bodies. The public PR report uses mechanism categories and observed counts, and
includes measurement limits. A private local JSON report includes case IDs and
provenance, so retain its output with private permissions:

```sh
cd backend
umask 077
uv run python scripts/report_repairs.py --database crucible.db <repair-id> > data/repair-report.json
```

The CLI reads durable role attempts (including invalid outputs and infrastructure
retries), executed trial observations, reported versus estimated tokens, cache
reuse and wall/execution intervals. Missing historical timings/cache records stay
unavailable. Reused evidence is not added to executed-job token totals. Provider
records do not prove a natural failure: `natural_failure_verified` remains false
until a human separately verifies the source incident. Deterministic fixtures and
scripted providers are plumbing checks, not natural-improvement measurements.

For the live demonstration, first pass deterministic stop/resume/restoration and
Docker containment checks. Choose an actual failed Run from normal work under the
unchanged running harness, invoke `/repair`, and retain its private report and
verified PR evidence. Do not seed a defect or change evaluation gates to obtain a
success. If no supported improvement is found, record that result. The user
reviews/merges the PR and restarts the updated harness. CI uses fake providers;
paid provider calls and real publication are separate user-invoked work.

# Crucible

## Model configuration

Crucible embeds LiteLLM `1.102.0` behind its provider-neutral model gateway. Set the
credential environment variable required by the selected LiteLLM provider (for
example `OPENAI_API_KEY`); secret values are read by the provider SDK and are never
written to Events, Context Manifests, or Task worktrees. Real-provider smoke tests
are opt-in; the default quality gate uses captured chunks and deterministic scripted
gateways only.

To opt into the live connectivity smoke for a provider/model pair, set
`CRUCIBLE_REAL_MODEL` plus that provider's credential and run
`uv run pytest tests/contract/models/test_real_model_smoke.py`. Passing this smoke
records support only for the exact configured pair; it performs no repository writes.

Select a model and its context budget before starting the backend:

```sh
export CRUCIBLE_MODEL=openai/gpt-5-mini
export CRUCIBLE_MODEL_INPUT_LIMIT=100000
export CRUCIBLE_MODEL_OUTPUT_RESERVE=4096
export CRUCIBLE_MAX_STEPS=20
export CRUCIBLE_MAX_TOOL_CALLS=100
export CRUCIBLE_MAX_MODEL_TOKENS=200000
export CRUCIBLE_MAX_ACTIVE_SECONDS=600
```

The model loop has a 20-Step default limit and admits at most 100 Tool Calls in one
batch. Each model request records a Context Manifest with the selected model,
estimated input size, root instruction digest, and Tool schema digest. If the input
budget cannot fit the required evidence, the Run fails with `context_limit`; if the
Step budget is exhausted, it fails with `budget_exhausted`.

The repository tools are `list_files`, `search_files`, `read_file`,
`write_file`, `apply_patch`, `workspace_status`, and `workspace_diff`. Paths are confined to the Task's
isolated worktree, reads and outputs are bounded, writes are atomic, and a complete
batch is validated before any call begins. `execute_command` adds an explicitly
approved Docker-only command boundary; no command process runs on the host.

## Approved Docker commands

Docker with a reachable daemon is required to provision Tasks and execute commands.
Each Task owns one labeled dependency volume mounted at
`/workspace/node_modules`. Every command is a structured executable plus argument
array—never a shell string—and receives its own durable Approval bound to the Tool
Call and the SHA-256 digest displayed in the UI. Denial terminalizes that call but
does not cancel the Run.

Approved containers run as `65532:65532` with a read-only root filesystem, all
capabilities dropped, `no-new-privileges`, bounded CPU/memory/PIDs/time/output, and
network `none` unless the displayed command explicitly requests outbound access.
Only the Task worktree and its verified dependency volume are mounted. Provider
credentials remain in the host model gateway and are never inherited by a command;
only explicit non-secret environment entries in the digest-bound spec are supplied,
and the browser reveals their names without exposing values.

Command stdout/stderr is independently bounded for live Events, model-facing text,
and retained evidence. Private content-addressed Artifacts live under
`CRUCIBLE_DATA_DIR/artifacts` (default `backend/data/artifacts`) with mode-0600
files and are retrieved through opaque Artifact IDs. Cancellation and restart
reconciliation stop/remove exact label-verified containers and never replay an
uncertain command. Orphaned or ownership-mismatched Docker resources are reported,
not automatically deleted.

To run the real lifecycle smoke, provide a locally available, non-root-compatible
image pinned by digest:

```sh
export CRUCIBLE_DOCKER_TEST_IMAGE='example/runner@sha256:...'
cd backend
uv run pytest -m docker tests/integration/sandbox/test_docker_lifecycle.py -q
```

This slice does not implement completion proposals, Validation/repair loops,
Acceptance, Result Revisions, Integration, steering, or compaction.

Crucible is an eval-driven, self-improving coding-agent harness for observable,
isolated repository-level software-engineering work.

## Backend

The backend requires CPython 3.13 and
[uv](https://docs.astral.sh/uv/). Run these commands from `backend/`.

Install dependencies:

```sh
uv sync
```

Run tests:

```sh
uv run pytest
```

Lint:

```sh
uv run ruff check .
```

Format or check formatting:

```sh
uv run ruff format .
uv run ruff format --check .
```

Type-check:

```sh
uv run mypy src/crucible
```

Start the development server:

```sh
uv run alembic upgrade head
uv run uvicorn crucible.api.app:create_app --factory --host 127.0.0.1 --reload
```

The server prints a high-entropy, one-time bootstrap secret at startup. Open
`http://127.0.0.1:8000/#bootstrap=<secret>` once to exchange it for an opaque,
revocable `HttpOnly; SameSite=Strict` browser session. The secret is removed
from the address bar after exchange; neither it nor the session credential is
stored in browser storage.

Production uses one origin by default (`http://127.0.0.1:8000`). For the Vite
development server, set `CRUCIBLE_DEVELOPMENT_ORIGIN` to its one exact origin,
for example `http://127.0.0.1:5173`; credentialed wildcard CORS is not enabled.
Set `CRUCIBLE_ORIGIN` to the exact API origin when changing the port. HTTPS
deployments must set `CRUCIBLE_SECURE_COOKIE=true`. All unsafe API requests
require that exact Origin plus the in-memory CSRF token issued at bootstrap.

## Milestone-one workflow

Repository registration is an explicit authenticated action and stores the
canonical Git root. Repository settings snapshot ordered Validation commands,
the pinned sandbox image/network policy, bounded Validation repairs, and the
Compaction threshold/model/prompt/attempt limits into each Run. Commands execute
only through the Approval-gated sandbox and keep bounded private evidence.

A Task owns one retained Conversation across Runs. Steering joins the active
Run at its next model boundary; a message after a terminal Run starts another
Run. Proactive Compaction summarizes only complete semantic units and preserves
the original Messages, traces, and Artifacts. Validation is harness-authored:
model completion claims never substitute for configured command outcomes, and
repair remains bounded inside the same Run.

Acceptance and Integration are separate explicit decisions. Acceptance creates
an immutable Result Revision in the isolated Task Workspace and leaves the
registered checkout unchanged. Integration requires a selected Result Revision,
the expected target ref/revision, and a clean tracked and untracked target. A
conflict is aborted and reported only when exact restoration is proven;
otherwise the durable state is `recovery_required`. Integrated Tasks retain the
Conversation, Validation evidence, Result Revisions, Artifacts, and Integration
history and can be reopened for review.

Run the deterministic milestone proof with:

```sh
make milestone
```

`make check` runs the complete offline quality gate, migration round-trip,
generated OpenAPI drift check, and production frontend build. `make smoke-docker`
is optional and requires a reachable Docker daemon and configured image. Real
model smoke tests are explicit opt-in (`make smoke-real-model`) and require model
credentials. Supported local operation requires Git, Python 3.13, Node.js 24,
pnpm, and Docker only for real sandbox execution.

Milestone one stops here. Slice 5—the Eval Runner, behavioral suites,
self-improvement campaigns, promotion, and rollback—is intentionally deferred.

## Frontend

The frontend requires Node.js 24 and pnpm. Run these commands from `frontend/`.

Install dependencies:

```sh
pnpm install
```

Run tests:

```sh
pnpm test --run
```

Lint:

```sh
pnpm lint
```

Type-check:

```sh
pnpm typecheck
```

Build:

```sh
pnpm build
```

Start the development server:

```sh
pnpm dev
```

## Complete local demonstration

Install and verify everything from the repository root:

```sh
make install
make check
```

Create a disposable committed repository to exercise Crucible without touching an
existing checkout:

```sh
mkdir -p /tmp/crucible-demo-repository
git -C /tmp/crucible-demo-repository init
git -C /tmp/crucible-demo-repository config user.email demo@example.com
git -C /tmp/crucible-demo-repository config user.name "Crucible Demo"
printf 'original\n' > /tmp/crucible-demo-repository/example.txt
git -C /tmp/crucible-demo-repository add example.txt
git -C /tmp/crucible-demo-repository commit -m fixture
```

In separate terminals run `make dev-backend` and `make dev-frontend`, then open the
URL printed by Vite. Register `/tmp/crucible-demo-repository`, explicitly create a
Task from `HEAD`, and send a Message. The page shows the queued/running/completed
Run lifecycle and the fake assistant response. Refreshing reconstructs the same
canonical Task and Conversation from SQLite. The Task uses a linked worktree under
`backend/data/workspaces`; confirm the registered checkout remains clean with:

```sh
git -C /tmp/crucible-demo-repository status --porcelain=v1
```

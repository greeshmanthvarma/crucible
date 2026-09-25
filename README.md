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

The Slice 2 repository tools are `list_files`, `search_files`, `read_file`,
`write_file`, `apply_patch`, `workspace_status`, and `workspace_diff`. Paths are confined to the Task's
isolated worktree, reads and outputs are bounded, writes are atomic, and a complete
batch is validated before any call begins. Only the worktree-root `AGENTS.md` is
loaded as repository instruction evidence in this slice. Nested instruction files,
shell commands, Docker, approval flows, and executable tools are intentionally out
of scope until later slices.

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
uv run uvicorn crucible.api.app:create_app --factory --reload
```

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

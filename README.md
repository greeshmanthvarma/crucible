# Crucible

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

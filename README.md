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

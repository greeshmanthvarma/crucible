.PHONY: install format lint typecheck test check dev-backend dev-frontend

install:
	cd backend && uv sync --frozen
	cd frontend && pnpm install --frozen-lockfile

format:
	cd backend && uv run ruff format src tests scripts
	cd frontend && pnpm exec prettier --write src tests package.json

lint:
	cd backend && uv run ruff format --check src tests scripts
	cd backend && uv run ruff check src tests scripts
	cd frontend && pnpm exec prettier --check src tests package.json
	cd frontend && pnpm lint

typecheck:
	cd backend && uv run mypy
	cd frontend && pnpm typecheck

test:
	cd backend && uv run pytest -q
	cd frontend && pnpm test --run

check: lint typecheck test
	cd backend && uv run python scripts/check_migrations.py
	cd frontend && pnpm generate:api
	git diff --exit-code -- frontend/src/api/schema.d.ts
	cd frontend && pnpm build

dev-backend:
	cd backend && uv run alembic upgrade head && uv run uvicorn crucible.api.app:create_app --factory --reload

dev-frontend:
	cd frontend && pnpm dev

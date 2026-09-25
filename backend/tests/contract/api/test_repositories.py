import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from crucible.api.app import create_app
from crucible.application.repository_service import RepositoryService
from crucible.storage.database import Database
from crucible.storage.unit_of_work import SqlAlchemyUnitOfWork, UnitOfWork
from crucible.workspaces.git import SubprocessGitClient


class FixedClock:
    def now(self) -> datetime:
        return datetime(2026, 9, 20, tzinfo=UTC)


def uow_factory(database: Database):
    def create() -> UnitOfWork:
        return SqlAlchemyUnitOfWork(database)

    return create


def create_repository(path: Path) -> None:
    path.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=path, check=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"], cwd=path, check=True
    )
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=path, check=True)
    (path / "README.md").write_text("fixture\n")
    subprocess.run(["git", "add", "README.md"], cwd=path, check=True)
    subprocess.run(["git", "commit", "-qm", "fixture"], cwd=path, check=True)


async def test_repository_registration_and_listing_contract(
    database: Database, tmp_path: Path
) -> None:
    root = tmp_path / "repository"
    create_repository(root)
    nested = root / "nested"
    nested.mkdir()
    service = RepositoryService(
        SubprocessGitClient(), uow_factory(database), FixedClock()
    )
    async with AsyncClient(
        transport=ASGITransport(app=create_app(service)), base_url="http://test"
    ) as client:
        created = await client.post("/api/repositories", json={"path": str(nested)})
        duplicate = await client.post("/api/repositories", json={"path": str(root)})
        listed = await client.get("/api/repositories")

    assert created.status_code == 201
    assert created.json()["rootPath"] == str(root.resolve())
    assert len(created.json()["headRevision"]) == 40
    assert duplicate.status_code == 200
    assert duplicate.json()["id"] == created.json()["id"]
    assert listed.status_code == 200
    assert listed.json() == [duplicate.json()]


async def test_invalid_repository_path_has_stable_error_contract(
    database: Database, tmp_path: Path
) -> None:
    service = RepositoryService(
        SubprocessGitClient(), uow_factory(database), FixedClock()
    )
    async with AsyncClient(
        transport=ASGITransport(app=create_app(service)), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/repositories", json={"path": str(tmp_path / "missing")}
        )

    assert response.status_code == 422
    assert response.json()["code"] == "repository_path_not_found"
    assert "does not exist" in response.json()["detail"]


async def test_repository_validation_settings_contract(
    database: Database, tmp_path: Path
) -> None:
    root = tmp_path / "settings-repository"
    create_repository(root)
    service = RepositoryService(
        SubprocessGitClient(), uow_factory(database), FixedClock()
    )
    async with AsyncClient(
        transport=ASGITransport(app=create_app(service)), base_url="http://test"
    ) as client:
        created = await client.post("/api/repositories", json={"path": str(root)})
        updated = await client.put(
            f"/api/repositories/{created.json()['id']}/settings",
            json={
                "validationCommands": [
                    {
                        "executable": "python",
                        "arguments": ["-m", "pytest"],
                        "cwd": ".",
                        "timeoutSeconds": 60,
                        "network": "none",
                        "environment": {"CI": "1"},
                        "image": "runner@sha256:" + "a" * 64,
                        "reason": "Authoritative tests",
                        "limits": {
                            "cpus": 1,
                            "memoryBytes": 1073741824,
                            "pids": 64,
                            "outputBytes": 100000,
                        },
                    }
                ],
                "validationRepairLimit": 2,
            },
        )

    assert updated.status_code == 200
    assert updated.json()["settings"]["validationCommands"][0]["arguments"] == [
        "-m",
        "pytest",
    ]


async def test_default_application_wires_repository_service(
    database: Database,
    database_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    del database
    root = tmp_path / "repository"
    create_repository(root)
    monkeypatch.setenv("CRUCIBLE_DATABASE_URL", database_url)
    monkeypatch.setenv("CRUCIBLE_ORIGIN", "http://test")
    monkeypatch.setattr("crucible.api.app.secrets.token_urlsafe", lambda _: "secret")
    app = create_app()

    async with app.router.lifespan_context(app):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            bootstrap = await client.post(
                "/api/auth/bootstrap", json={"secret": "secret"}
            )
            response = await client.post(
                "/api/repositories",
                json={"path": str(root)},
                headers={
                    "Origin": "http://test",
                    "X-CSRF-Token": bootstrap.json()["csrfToken"],
                },
            )

    assert response.status_code == 201

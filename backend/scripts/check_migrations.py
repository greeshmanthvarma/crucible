import tempfile
from pathlib import Path

from alembic import command
from alembic.config import Config


def main() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as temporary:
        path = Path(temporary.name)
    try:
        config = Config(Path(__file__).parents[1] / "alembic.ini")
        config.set_main_option("sqlalchemy.url", f"sqlite+aiosqlite:///{path}")
        command.upgrade(config, "head")
        command.downgrade(config, "base")
        command.upgrade(config, "head")
    finally:
        path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()

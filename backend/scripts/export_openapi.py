import json
import sys
from pathlib import Path

from crucible.api.app import create_app


def main() -> None:
    destination = Path(sys.argv[1])
    destination.write_text(json.dumps(create_app().openapi(), sort_keys=True) + "\n")


if __name__ == "__main__":
    main()

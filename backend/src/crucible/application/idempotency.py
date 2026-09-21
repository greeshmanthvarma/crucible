import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


def canonical_request_hash(payload: dict[str, object]) -> str:
    encoded = json.dumps(
        {"schema_version": 1, **payload},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class IdempotencyRecord:
    id: UUID
    scope: str
    key: str
    request_hash: str
    response_status: int
    response_json: dict[str, object]
    created_at: datetime

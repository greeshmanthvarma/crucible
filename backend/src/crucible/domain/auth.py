from dataclasses import dataclass, replace
from datetime import datetime

from crucible.domain.clock import require_utc


@dataclass(frozen=True)
class BrowserSession:
    session_hash: str
    csrf_hash: str
    created_at: datetime
    expires_at: datetime
    revoked_at: datetime | None = None

    def __post_init__(self) -> None:
        require_utc(self.created_at, self.expires_at, self.revoked_at)

    def is_active(self, now: datetime) -> bool:
        return self.revoked_at is None and now < self.expires_at

    def revoke(self, now: datetime) -> "BrowserSession":
        return replace(self, revoked_at=now)

    def rotate_csrf(self, csrf_hash: str) -> "BrowserSession":
        return replace(self, csrf_hash=csrf_hash)

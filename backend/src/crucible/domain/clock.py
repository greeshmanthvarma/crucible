from datetime import UTC, datetime
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


def require_utc(*values: datetime | None) -> None:
    for value in values:
        if value is not None and value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("durable timestamps must be timezone-aware UTC values")

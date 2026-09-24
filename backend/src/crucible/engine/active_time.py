import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator


class ActiveTimeBudget:
    """A deadline that can be suspended while a Run awaits human authority."""

    def __init__(self, seconds: float) -> None:
        if seconds <= 0:
            raise ValueError("Active time budget must be positive")
        self._remaining = seconds
        self._timeout: asyncio.Timeout | None = None
        self._active_since: float | None = None
        self._paused = False

    @asynccontextmanager
    async def track(self) -> AsyncIterator[None]:
        loop = asyncio.get_running_loop()
        self._active_since = loop.time()
        async with asyncio.timeout(self._remaining) as timeout:
            self._timeout = timeout
            try:
                yield
            finally:
                if not self._paused and self._active_since is not None:
                    self._remaining -= max(0, loop.time() - self._active_since)
                self._timeout = None
                self._active_since = None

    @asynccontextmanager
    async def pause(self) -> AsyncIterator[None]:
        if self._paused:
            yield
            return
        self._paused = True
        loop = asyncio.get_running_loop()
        if self._timeout is not None and self._active_since is not None:
            self._remaining -= max(0, loop.time() - self._active_since)
            self._timeout.reschedule(None)
        try:
            yield
        finally:
            self._active_since = loop.time()
            if self._timeout is not None:
                self._timeout.reschedule(self._active_since + max(0, self._remaining))
            self._paused = False

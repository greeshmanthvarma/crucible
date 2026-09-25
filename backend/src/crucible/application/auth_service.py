import hashlib
import hmac
import secrets
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta

from crucible.application.errors import AuthenticationRequired, CsrfRejected
from crucible.application.ports import UnitOfWork
from crucible.domain.auth import BrowserSession
from crucible.domain.clock import Clock


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


@dataclass(frozen=True)
class IssuedSession:
    session_token: str
    csrf_token: str


class AuthService:
    def __init__(
        self,
        unit_of_work: Callable[[], UnitOfWork],
        clock: Clock,
        bootstrap_secret: str,
        *,
        ttl: timedelta = timedelta(hours=8),
    ) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._bootstrap_hash = _digest(bootstrap_secret)
        self._bootstrap_consumed = False
        self._ttl = ttl

    async def exchange(self, bootstrap_secret: str) -> IssuedSession:
        supplied = _digest(bootstrap_secret)
        if self._bootstrap_consumed or not hmac.compare_digest(
            supplied, self._bootstrap_hash
        ):
            raise AuthenticationRequired("Bootstrap credential is invalid or consumed")
        self._bootstrap_consumed = True
        session_token = secrets.token_urlsafe(32)
        csrf_token = secrets.token_urlsafe(32)
        now = self._clock.now()
        async with self._unit_of_work() as uow:
            await uow.browser_sessions.add(
                BrowserSession(
                    _digest(session_token),
                    _digest(csrf_token),
                    now,
                    now + self._ttl,
                )
            )
            await uow.commit()
        return IssuedSession(session_token, csrf_token)

    async def authenticate(
        self, session_token: str | None, csrf_token: str | None = None
    ) -> BrowserSession:
        if not session_token:
            raise AuthenticationRequired("Browser session is required")
        async with self._unit_of_work() as uow:
            session = await uow.browser_sessions.get(_digest(session_token))
        if session is None or not session.is_active(self._clock.now()):
            raise AuthenticationRequired("Browser session is invalid or expired")
        if csrf_token is not None and not hmac.compare_digest(
            session.csrf_hash, _digest(csrf_token)
        ):
            raise CsrfRejected("CSRF token is invalid")
        return session

    async def revoke(self, session_token: str) -> None:
        session = await self.authenticate(session_token)
        async with self._unit_of_work() as uow:
            await uow.browser_sessions.update(session.revoke(self._clock.now()))
            await uow.commit()

    async def refresh_csrf(self, session_token: str | None) -> str:
        session = await self.authenticate(session_token)
        csrf_token = secrets.token_urlsafe(32)
        async with self._unit_of_work() as uow:
            await uow.browser_sessions.update(session.rotate_csrf(_digest(csrf_token)))
            await uow.commit()
        return csrf_token

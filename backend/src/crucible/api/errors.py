from fastapi import Request
from fastapi.responses import JSONResponse

from crucible.application.errors import ApplicationError


async def application_error_handler(
    request: Request, error: ApplicationError
) -> JSONResponse:
    return JSONResponse(
        status_code=error.status_code,
        content={"code": error.code, "detail": error.detail},
    )

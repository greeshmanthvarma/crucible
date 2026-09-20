from fastapi import Request
from fastapi.responses import JSONResponse

from crucible.application.errors import ApplicationError


async def application_error_handler(
    request: Request, error: ApplicationError
) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={"code": error.code, "detail": error.detail},
    )

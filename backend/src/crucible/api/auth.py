from typing import Annotated

from fastapi import APIRouter, Cookie, Request, Response, status

from crucible.api.schemas import BootstrapRequest, BootstrapResponse

router = APIRouter(tags=["auth"])


@router.post(
    "/api/auth/bootstrap",
    response_model=BootstrapResponse,
    status_code=status.HTTP_201_CREATED,
)
async def bootstrap(
    request: Request, body: BootstrapRequest, response: Response
) -> BootstrapResponse:
    issued = await request.app.state.auth_service.exchange(body.secret)
    response.set_cookie(
        request.app.state.session_cookie_name,
        issued.session_token,
        httponly=True,
        secure=request.app.state.secure_cookie,
        samesite="strict",
        path="/",
    )
    return BootstrapResponse(csrf_token=issued.csrf_token)


@router.get("/api/auth/session", response_model=BootstrapResponse)
async def resume_session(
    request: Request,
    session_token: Annotated[str | None, Cookie(alias="crucible_session")] = None,
) -> BootstrapResponse:
    return BootstrapResponse(
        csrf_token=await request.app.state.auth_service.refresh_csrf(session_token)
    )


@router.post("/api/auth/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    request: Request,
    response: Response,
    session_token: Annotated[str | None, Cookie(alias="crucible_session")] = None,
) -> None:
    if session_token is not None:
        await request.app.state.auth_service.revoke(session_token)
    response.delete_cookie(
        request.app.state.session_cookie_name,
        httponly=True,
        secure=request.app.state.secure_cookie,
        samesite="strict",
        path="/",
    )

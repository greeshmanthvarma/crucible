from fastapi import FastAPI


def create_app() -> FastAPI:
    app = FastAPI(title="Crucible", version="0.1.0")

    @app.get("/api/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app

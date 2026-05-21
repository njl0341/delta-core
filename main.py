import uvicorn

from src.delta_core.api import app

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",  # noqa: S104 — bind address configurable via env in prod
        port=8000,
        reload=False,
    )

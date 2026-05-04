from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import cast

from f1_polymarket_lab.common import utc_now
from f1_polymarket_lab.common.settings import get_settings
from f1_polymarket_lab.storage.db import session_factory
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import Engine

from f1_polymarket_api.api.v1.action_routes import action_router
from f1_polymarket_api.api.v1.routes import router as api_router
from f1_polymarket_api.schemas import ApiHealthResponse


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    app.state.settings = settings
    app.state.session_maker = session_factory(settings.database_url)
    try:
        yield
    finally:
        engine = cast(Engine, app.state.session_maker.kw["bind"])
        engine.dispose()


app = FastAPI(title="f1-polymarket-lab api", version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:3002",
        "http://127.0.0.1:3002",
    ],
    allow_origin_regex=r"^https?://.*$",
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
app.include_router(api_router)
app.include_router(action_router)


@app.get("/health", response_model=ApiHealthResponse)
def health() -> ApiHealthResponse:
    return ApiHealthResponse(service="api", status="ok", now=utc_now())

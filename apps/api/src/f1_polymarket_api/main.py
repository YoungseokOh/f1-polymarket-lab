from __future__ import annotations

from f1_polymarket_lab.common import utc_now
from fastapi import FastAPI

from f1_polymarket_api.api.v1.routes import router as api_router
from f1_polymarket_api.schemas import ApiHealthResponse

app = FastAPI(title="f1-polymarket-lab api", version="0.1.0")
app.include_router(api_router)


@app.get("/health", response_model=ApiHealthResponse)
def health() -> ApiHealthResponse:
    return ApiHealthResponse(service="api", status="ok", now=utc_now())

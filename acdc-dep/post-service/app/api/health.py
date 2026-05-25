from fastapi import APIRouter

from ..schemas.api import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health/live", response_model=HealthResponse)
async def live() -> HealthResponse:
    return HealthResponse(status="ok")


@router.get("/health/ready", response_model=HealthResponse)
async def ready() -> HealthResponse:
    # db_app connectivity could be checked here in the future
    return HealthResponse(status="ok")

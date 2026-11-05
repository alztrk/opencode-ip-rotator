import logging
from fastapi import APIRouter

from services.discovery import get_discovered_models

log = logging.getLogger("routes.models")
router = APIRouter()


@router.get("/v1/models")
async def list_models():
    models = get_discovered_models()
    return {
        "object": "list",
        "data": models,
    }

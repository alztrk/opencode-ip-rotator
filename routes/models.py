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
        "data": [
            {
                "id": m["id"],
                "object": "model",
                "created": m.get("created") or 1700000000,
                "owned_by": m.get("owned_by") or "opencode",
                "name": m.get("name") or m["id"],
            }
            for m in models
        ],
    }

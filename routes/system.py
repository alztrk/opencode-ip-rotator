import os
import time
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from core.db import get_recent_ips, load_all_usage
from core.metrics import STARTUP_TIME, metrics
from rotator import get_ip_location, get_public_ip, rotation_count
from services.discovery import get_discovered_models

router = APIRouter()
templates = Jinja2Templates(directory=str(Path("templates")))


@router.get("/health")
@router.get("/status")
async def health_check():
    models = get_discovered_models()
    return {
        "status": "healthy",
        "timestamp": int(time.time()),
        "uptime_seconds": int(time.time() - STARTUP_TIME),
        "discovered_models_count": len(models),
    }


@router.get("/metrics")
async def get_metrics(request: Request):
    accept = request.headers.get("accept", "")
    if "text/plain" in accept or "openmetrics" in accept:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    # JSON metrics for test.py and dashboard
    ip = get_public_ip()
    loc = get_ip_location(ip) if ip != "Unknown" else {}
    models = get_discovered_models()
    return {
        "total_requests": metrics["total_requests"],
        "successful_requests": metrics["successful_requests"],
        "rate_limited_requests": metrics["rate_limited_requests"],
        "failed_requests": metrics["failed_requests"],
        "rotation_count": rotation_count,
        "verified_public_ip": ip,
        "location": loc,
        "uptime_seconds": int(time.time() - STARTUP_TIME),
        "discovered_models": [m["id"] for m in models],
    }


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "metrics": metrics},
    )


@router.get("/dashboard/stats")
async def dashboard_stats():
    ip = get_public_ip()
    loc = get_ip_location(ip) if ip != "Unknown" else {}
    models = get_discovered_models()
    return {
        "verified_ip": ip,
        "location": loc,
        "total_requests": metrics["total_requests"],
        "successful_requests": metrics["successful_requests"],
        "rate_limited_requests": metrics["rate_limited_requests"],
        "rotation_count": rotation_count,
        "uptime_seconds": int(time.time() - STARTUP_TIME),
        "active_models_count": len(models),
        "models": models,
        "model_usage": load_all_usage(),
        "recent_ips": get_recent_ips(limit=10),
    }

"""Discovery and normalization of OpenCode's free model catalog.

Periodically fetches models from OpenCode API and filters models whose text token
pricing is free (input.price == 0 and output.price == 0).
"""

import json
import logging
import os
import threading
from typing import Any, Dict, List, Optional
from urllib.request import Request as UrlRequest, urlopen

from core.config import (
    DEFAULT_MODEL,
    DEFAULT_MODEL_OVERRIDE,
    MODEL_DISCOVERY_INTERVAL,
    MODELS_URL,
)
from core.metrics import metrics

log = logging.getLogger("services.discovery")

CATALOG_HEADERS = {
    "Accept": "application/json",
    "Authorization": "Bearer public",
    "User-Agent": "OpenCode-IP-Rotator/1.0",
}

_discovered_models: List[Dict[str, Any]] = []
_discovery_lock = threading.Lock()
_stop_event = threading.Event()


def _zero_price(value: Any) -> bool:
    """Return True only for numeric zero (excluding booleans)."""
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value == 0


def is_free_model(model: Dict[str, Any]) -> bool:
    """Return True when the catalog entry costs nothing for text tokens."""
    cost = model.get("cost")
    if not isinstance(cost, dict):
        return False
    text = cost.get("text")
    if not isinstance(text, dict):
        return False

    input_cost = text.get("input")
    output_cost = text.get("output")
    if not isinstance(input_cost, dict) or not isinstance(output_cost, dict):
        return False

    return _zero_price(input_cost.get("price")) and _zero_price(output_cost.get("price"))


def parse_free_models(payload: Any) -> List[Dict[str, Any]]:
    """Filter a catalog payload while preserving each selected model record."""
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if not isinstance(data, list):
        return []

    free_models: List[Dict[str, Any]] = []
    seen = set()
    for model in data:
        if not isinstance(model, dict):
            continue
        model_id = model.get("id")
        if not isinstance(model_id, str) or not model_id or model_id in seen:
            continue
        model_type = model.get("type")
        if model_type not in (None, "chat"):
            continue
        if not is_free_model(model):
            continue
        seen.add(model_id)
        free_models.append(dict(model))
    return free_models


def fetch_free_models(
    url: str = MODELS_URL,
    timeout: float = 10.0,
    opener=urlopen,
) -> List[Dict[str, Any]]:
    """Fetch the catalog and return its free models."""
    request = UrlRequest(url, headers=CATALOG_HEADERS)
    with opener(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return parse_free_models(payload)


def get_discovered_models() -> List[Dict[str, Any]]:
    with _discovery_lock:
        return list(_discovered_models)


def get_default_model() -> str:
    if DEFAULT_MODEL_OVERRIDE:
        return DEFAULT_MODEL_OVERRIDE
    with _discovery_lock:
        if _discovered_models:
            return _discovered_models[0]["id"]
    return DEFAULT_MODEL


def refresh_models_once() -> bool:
    global _discovered_models
    try:
        new_models = fetch_free_models(MODELS_URL, timeout=10)
    except Exception as exc:
        log.warning(
            "Auto-discovery failed (%s); keeping %d last-known models.",
            exc,
            len(_discovered_models),
        )
        return False

    if not new_models:
        log.warning(
            "Auto-discovery returned 0 free models from %s; keeping existing list.",
            MODELS_URL,
        )
        return False

    with _discovery_lock:
        _discovered_models = new_models
        metrics["discovered_models_count"] = len(new_models)

    log.info("Auto-discovery refreshed: %d free models available.", len(new_models))
    return True


def discovery_worker() -> None:
    while not _stop_event.is_set():
        refresh_models_once()
        _stop_event.wait(MODEL_DISCOVERY_INTERVAL)


def start_discovery() -> None:
    _stop_event.clear()
    refresh_models_once()
    thread = threading.Thread(target=discovery_worker, daemon=True)
    thread.start()


def stop_discovery() -> None:
    _stop_event.set()

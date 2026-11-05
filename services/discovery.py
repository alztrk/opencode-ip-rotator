import logging
import threading
import time
from typing import Any, Dict, List

from core.config import (
    DEFAULT_MODEL,
    DEFAULT_MODEL_OVERRIDE,
    MODEL_DISCOVERY_INTERVAL,
    MODELS_URL,
)
from core.metrics import metrics
from model_catalog import fetch_free_models

log = logging.getLogger("services.discovery")

_discovered_models: List[Dict[str, Any]] = []
_discovery_lock = threading.Lock()
_stop_event = threading.Event()


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

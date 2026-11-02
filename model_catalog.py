"""Discovery and normalization of OpenCode's model catalog.

The catalog exposes rich metadata for every model, including token pricing.
A model is considered free only when both input and output text token prices
are zero; the model id is deliberately not used as a signal because some free
models (for example ``big-pickle``) do not contain the word "free".
"""

import json
import os
from typing import Any, Dict, List, Optional
from urllib.request import Request as UrlRequest, urlopen

DEFAULT_MODELS_URL = "https://opencode.ai/inference/v1/models"
MODELS_URL = os.environ.get("OPENCODE_MODELS_URL", DEFAULT_MODELS_URL)

CATALOG_HEADERS = {
    "Accept": "application/json",
    "Authorization": "Bearer public",
    "User-Agent": "OpenCode-IP-Rotator/1.0",
}


def _zero_price(value: Any) -> bool:
    """Return True only for a real numeric zero.

    ``False == 0`` in Python, so booleans must be rejected explicitly.
    """
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


def normalize_model(model: Dict[str, Any]) -> Dict[str, Any]:
    """Keep the upstream fields the proxy and its clients actually need."""
    api = model.get("api")
    api = api if isinstance(api, dict) else {}
    chat = api.get("chat-completions")
    chat = chat if isinstance(chat, dict) else {}

    return {
        "id": model["id"],
        "name": model.get("name") or model["id"],
        "created": model.get("created"),
        "owned_by": model.get("owned_by") or "opencode",
        "base_url": chat.get("base_url"),
    }


def parse_free_models(payload: Any) -> List[Dict[str, Any]]:
    """Filter a catalog payload down to normalized free chat models."""
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
        free_models.append(normalize_model(model))
    return free_models


def fetch_free_models(
    url: str = MODELS_URL,
    timeout: float = 10.0,
    opener=urlopen,
) -> List[Dict[str, Any]]:
    """Fetch the catalog and return its free models.

    Network/protocol errors propagate so callers can keep their last-known-good
    list instead of replacing it with an empty result.
    """
    request = UrlRequest(url, headers=CATALOG_HEADERS)
    with opener(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return parse_free_models(payload)

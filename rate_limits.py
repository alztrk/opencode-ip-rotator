import json


def _raw_body(response) -> object:
    """Return the response body for streaming responses too.

    ``curl_cffi`` keeps the body unread on ``stream=True`` responses, so
    ``.content`` is empty until the body is consumed explicitly.
    """
    raw = getattr(response, "content", None)
    if raw:
        return raw
    for reader in ("read", "iter_content"):
        method = getattr(response, reader, None)
        if not callable(method):
            continue
        try:
            result = method() if reader == "read" else b"".join(method())
        except Exception:
            continue
        if result:
            return result
    return raw


def _response_payload(response, error_type: str = "rate_limit", default_message: str = "") -> object:
    raw = _raw_body(response)
    if raw:
        try:
            return json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
        except (UnicodeDecodeError, ValueError):
            pass
    try:
        return response.json()
    except Exception:
        pass
    try:
        body = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else (raw or "")
    except Exception:
        body = ""
    if not body:
        try:
            body = response.text
        except Exception:
            body = ""
    return {"error": {"type": error_type, "message": body or default_message}}


def response_payload(
    response,
    error_type: str = "upstream_error",
    default_message: str = "",
) -> object:
    """Read an upstream body, including ``stream=True`` responses."""
    return _response_payload(response, error_type, default_message)


def classify_upstream_429(response) -> tuple[str, int | None, object]:
    """Preserve upstream limit semantics without exposing an internal error type."""
    retry_after = response.headers.get("retry-after")
    try:
        retry_seconds = max(0, int(float(retry_after))) if retry_after else None
    except (TypeError, ValueError):
        retry_seconds = None
    payload = _response_payload(response)

    error = payload.get("error", {}) if isinstance(payload, dict) else {}
    error_type = str(error.get("type", "rate_limit"))
    if error_type in {"FreeUsageLimitError", "GoUsageLimitError", "BlackUsageLimitError"}:
        category = "quota"
    elif error_type == "RateLimitError":
        category = "rate_limit"
    else:
        category = "rate_limit"
    return category, retry_seconds, payload

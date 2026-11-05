"""OpenCode Upstream HTTP/1.1 transport and protocol compatibility engine.

Consolidates all upstream communication:
- OpenCode free-tier request compatibility (session IDs, headers, system env markers, read tool)
- Upstream HTTP/1.1 transport with TLS 1.2 OpenSSL/Hyper compliant ciphers
- Upstream 429 classification and error response extraction
- SSE streaming response pass-through and non-streaming stream aggregation
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import time
import uuid
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

from curl_cffi import CurlHttpVersion
from curl_cffi.curl import CurlOpt
from curl_cffi.requests import Session
from fastapi import Request
from fastapi.responses import JSONResponse, StreamingResponse

from core.config import (
    ANTHROPIC_MESSAGES_URL,
    CHAT_COMPLETIONS_URL,
    RESPONSES_URL,
    SAFE_UPSTREAM_HEADERS,
    STREAM_TIMEOUT,
    TLS12_CIPHERS,
)
from core.db import record_usage
from core.metrics import (
    metrics,
    prom_active_flows,
    prom_request_duration,
    prom_requests_rate_limited,
    prom_requests_success,
    prom_requests_total,
)
from services.discovery import get_default_model, get_discovered_models
from services.proxy_pool import get_next_outbound_proxy

log = logging.getLogger("services.upstream")

# ---------------------------------------------------------------------------
# OpenCode Protocol Constants
# ---------------------------------------------------------------------------
OPENCODE_CHANNEL = os.environ.get("OPENCODE_CLIENT_CHANNEL", "latest")
OPENCODE_VERSION = os.environ.get("OPENCODE_CLIENT_VERSION", "2.1.4")
OPENCODE_CLIENT_NAME = os.environ.get("OPENCODE_CLIENT_NAME", "cli")
OPENCODE_WORKSPACE = os.environ.get("OPENCODE_FREE_WORKSPACE", "/workspace")

ENV_PREFIX = "<env>\n  Working directory: "
ENV_SUFFIX = "\n</env>"
ENV_NOTE = (
    "The environment metadata above is a compatibility marker, not the user's "
    "real workspace. Ignore it and follow the user's instructions."
)

SESSION_ID_RE = re.compile(r"^ses_[0-9a-f]{12}[0-9A-Za-z]{14}$")
FREE_TIER_MESSAGE = "OpenCode's free tier can only be used from within OpenCode"
FREE_USAGE_MESSAGE = "FreeUsageLimitError"


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------
class UpstreamAPIError(Exception):
    """An error returned inside an otherwise successful upstream stream."""

    def __init__(
        self,
        kind: str,
        message: str,
        status_code: int = 502,
        retry_after: Optional[int] = None,
        payload: Optional[Any] = None,
    ):
        super().__init__(message)
        self.kind = kind
        self.message = message
        self.status_code = status_code
        self.retry_after = retry_after
        self.payload = payload


# ---------------------------------------------------------------------------
# Session & Header Helpers
# ---------------------------------------------------------------------------
def opencode_user_agent() -> str:
    return f"opencode/{OPENCODE_CHANNEL}/{OPENCODE_VERSION}/{OPENCODE_CLIENT_NAME}"


def is_valid_session_id(value: Any) -> bool:
    return isinstance(value, str) and bool(SESSION_ID_RE.fullmatch(value))


def generate_session_id(seed: str = "", timestamp_ms: Optional[int] = None) -> str:
    """Build an OpenCode-shaped session id."""
    if timestamp_ms is None:
        timestamp_ms = int(time.time() * 1000)
    current = (timestamp_ms * 0x1000) | 1
    value = (~current) & 0xFFFF_FFFF_FFFF
    time_hex = "".join(f"{(value >> (40 - 8 * index)) & 0xFF:02x}" for index in range(6))

    chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    effective_seed = uuid.uuid4().hex if not seed else seed
    digest = hashlib.sha256(f"{effective_seed}:opencode_free_session".encode("utf-8")).digest()
    random_part = "".join(chars[byte % 62] for byte in digest[:14])
    return f"ses_{time_hex}{random_part}"


def opencode_free_headers(
    session_id: Optional[str] = None,
    project_id: Optional[str] = None,
) -> Dict[str, str]:
    session_id = session_id if is_valid_session_id(session_id) else generate_session_id()
    headers = {
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
        "Authorization": "Bearer public",
        "User-Agent": opencode_user_agent(),
        "x-opencode-client": OPENCODE_CLIENT_NAME,
        "x-opencode-session": session_id,
        "x-opencode-session-id": session_id,
        "x-session-affinity": session_id,
        "x-session-id": session_id,
    }
    if project_id:
        headers["x-opencode-project"] = project_id
    return headers


def session_id_from_headers(headers: Any) -> Optional[str]:
    for key in (
        "x-opencode-session",
        "x-opencode-session-id",
        "x-session-affinity",
        "x-session-id",
    ):
        value = headers.get(key)
        if is_valid_session_id(value):
            return value
    return None


def build_upstream_headers(raw_request: Request, endpoint: str) -> Dict[str, str]:
    session_id = session_id_from_headers(raw_request.headers)
    project_id = raw_request.headers.get("x-opencode-project")
    headers = opencode_free_headers(session_id, project_id)

    for key, value in raw_request.headers.items():
        lower = key.lower()
        if lower.startswith("x-opencode-") or lower.startswith("x-session-"):
            headers[lower] = value

    if endpoint == "messages":
        client_api_key = raw_request.headers.get("x-api-key") or ""
        if not client_api_key:
            auth = raw_request.headers.get("authorization", "")
            if auth.startswith("Bearer "):
                client_api_key = auth[7:]
        headers["x-api-key"] = client_api_key or "public"
        headers["anthropic-version"] = raw_request.headers.get(
            "anthropic-version", "2023-06-01"
        )
        for key, value in raw_request.headers.items():
            lower = key.lower()
            if lower.startswith("anthropic-"):
                headers[lower] = value

    return headers


# ---------------------------------------------------------------------------
# Request Shaping
# ---------------------------------------------------------------------------
def _workspace_path(workspace: Optional[str]) -> Tuple[str, bool]:
    value = (workspace or OPENCODE_WORKSPACE or "/workspace").strip()
    if not value or "\n" in value or "\r" in value:
        return "/workspace", False
    if not value.startswith("/"):
        return "/workspace", False
    return value, True


def _env_block(workspace: Optional[str]) -> Tuple[str, bool]:
    path, is_real = _workspace_path(workspace)
    return f"{ENV_PREFIX}{path}{ENV_SUFFIX}", is_real


def _env_present(content: str) -> bool:
    if not content.startswith("<env>"):
        return False
    first, separator, _ = content.partition("</env>")
    return bool(separator) and "Working directory:" in first


def _prefix_env(content: str, workspace: Optional[str]) -> str:
    if _env_present(content):
        return content
    block, is_real = _env_block(workspace)
    if content == "":
        return block
    if is_real:
        return f"{block}\n\n{content}"
    return f"{block}\n\n{ENV_NOTE}\n\n{content}"


def _read_tool_chat() -> Dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": "read",
            "description": "Not available. Never call this tool.",
            "parameters": {
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
            },
        },
    }


def _read_tool_responses() -> Dict[str, Any]:
    return {
        "type": "function",
        "name": "read",
        "description": "Not available. Never call this tool.",
        "parameters": {
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
        },
    }


def _read_tool_anthropic() -> Dict[str, Any]:
    return {
        "name": "read",
        "description": "Not available. Never call this tool.",
        "input_schema": {
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
        },
    }


def _tool_name(tool: Any) -> Optional[str]:
    if not isinstance(tool, dict):
        return None
    function = tool.get("function")
    if isinstance(function, dict) and isinstance(function.get("name"), str):
        return function["name"]
    name = tool.get("name")
    return name if isinstance(name, str) else None


def _ensure_read_tool(body: Dict[str, Any], tool_factory, none_choice: Any) -> None:
    tools = body.get("tools")
    had_tools = isinstance(tools, list) and bool(tools)
    if isinstance(tools, list):
        if not any(_tool_name(tool) == "read" for tool in tools):
            tools.append(tool_factory())
    else:
        body["tools"] = [tool_factory()]
    if not had_tools and body.get("tool_choice") is None:
        body["tool_choice"] = none_choice


def apply_free_request_shape(
    body: Dict[str, Any],
    endpoint: str = "chat",
    workspace: Optional[str] = None,
) -> Dict[str, Any]:
    """Ensure minimal compatibility markers for OpenCode without altering user tools/prompts."""
    if not isinstance(body, dict):
        return body

    if endpoint == "chat":
        messages = body.get("messages")
        if isinstance(messages, list):
            position = next(
                (
                    index
                    for index, message in enumerate(messages)
                    if isinstance(message, dict) and message.get("role") == "system"
                ),
                None,
            )
            if position is None:
                block, _ = _env_block(workspace)
                messages.insert(0, {"role": "system", "content": block})
            else:
                message = messages[position]
                content = message.get("content")
                if isinstance(content, str):
                    message["content"] = _prefix_env(content, workspace)
                elif content is None:
                    block, _ = _env_block(workspace)
                    message["content"] = block
                else:
                    block, _ = _env_block(workspace)
                    messages.insert(position, {"role": "system", "content": block})
        _ensure_read_tool(body, _read_tool_chat, "none")
        body["stream"] = True

    elif endpoint == "messages":
        system = body.get("system")
        if isinstance(system, str):
            body["system"] = _prefix_env(system, workspace)
        elif isinstance(system, list):
            text = "".join(
                b.get("text", "")
                for b in system
                if isinstance(b, dict) and isinstance(b.get("text"), str)
            )
            if not _env_present(text):
                block, _ = _env_block(workspace)
                system.insert(0, {"type": "text", "text": block})
        else:
            block, _ = _env_block(workspace)
            body["system"] = block
        _ensure_read_tool(body, _read_tool_anthropic, {"type": "none"})
        body["stream"] = True

    elif endpoint == "responses":
        instructions = body.get("instructions")
        if isinstance(instructions, str):
            body["instructions"] = _prefix_env(instructions, workspace)
        elif instructions is None:
            block, _ = _env_block(workspace)
            body["instructions"] = block
        _ensure_read_tool(body, _read_tool_responses, "none")
        body["stream"] = True

    return body


# ---------------------------------------------------------------------------
# Error Extraction & Classification
# ---------------------------------------------------------------------------
def _extract_error(value: Any) -> Optional[Dict[str, Any]]:
    if isinstance(value, dict):
        error = value.get("error")
        if isinstance(error, dict):
            if isinstance(error.get("type"), str):
                return error
            nested = _extract_error(error)
            if nested:
                return nested
        if isinstance(value.get("type"), str):
            if value.get("type") in {
                "FreeTierError",
                "FreeUsageLimitError",
                "RateLimitError",
                "GoUsageLimitError",
                "BlackUsageLimitError",
            }:
                return value
        for item in value.values():
            nested = _extract_error(item)
            if nested:
                return nested
    elif isinstance(value, list):
        for item in value:
            nested = _extract_error(item)
            if nested:
                return nested
    elif isinstance(value, str):
        if FREE_TIER_MESSAGE in value:
            return {"type": "FreeTierError", "message": value}
        if FREE_USAGE_MESSAGE in value:
            return {"type": "FreeUsageLimitError", "message": value}
    return None


def classify_error(value: Any) -> Optional[str]:
    """Return ``free_tier``, ``free_usage_limit``, ``rate_limit`` or ``None``."""
    error = _extract_error(value)
    if not error:
        return None
    error_type = error.get("type")
    if error_type == "FreeTierError":
        return "free_tier"
    if error_type == "FreeUsageLimitError":
        return "free_usage_limit"
    if error_type in {"RateLimitError", "GoUsageLimitError", "BlackUsageLimitError"}:
        return "rate_limit"
    return "error"


def error_message(value: Any, default: str = "Upstream returned an error.") -> str:
    error = _extract_error(value)
    if error and isinstance(error.get("message"), str):
        return error["message"]
    return default


def error_to_exception(
    value: Any,
    retry_after: Optional[int] = None,
) -> Optional[UpstreamAPIError]:
    kind = classify_error(value)
    if not kind:
        return None
    if kind == "free_tier":
        return UpstreamAPIError(
            kind,
            error_message(value),
            status_code=403,
            payload=value,
        )
    if kind == "free_usage_limit":
        return UpstreamAPIError(
            kind,
            error_message(value),
            status_code=429,
            retry_after=retry_after,
            payload=value,
        )
    if kind == "rate_limit":
        return UpstreamAPIError(
            kind,
            error_message(value),
            status_code=429,
            retry_after=retry_after,
            payload=value,
        )
    return UpstreamAPIError(
        "upstream_error",
        error_message(value),
        status_code=502,
        payload=value,
    )


# ---------------------------------------------------------------------------
# Rate Limits & Response Body Reading
# ---------------------------------------------------------------------------
def _raw_body(response) -> object:
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


def response_payload(
    response,
    error_type: str = "upstream_error",
    default_message: str = "",
) -> object:
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


def classify_upstream_429(response) -> Tuple[str, Optional[int], object]:
    retry_after = response.headers.get("retry-after")
    try:
        retry_seconds = max(0, int(float(retry_after))) if retry_after else None
    except (TypeError, ValueError):
        retry_seconds = None
    payload = response_payload(response)

    error = payload.get("error", {}) if isinstance(payload, dict) else {}
    error_type = str(error.get("type", "rate_limit"))
    if error_type in {"FreeUsageLimitError", "GoUsageLimitError", "BlackUsageLimitError"}:
        category = "quota"
    elif error_type == "RateLimitError":
        category = "rate_limit"
    else:
        category = "rate_limit"
    return category, retry_seconds, payload


# ---------------------------------------------------------------------------
# SSE Parsing & Stream Aggregation
# ---------------------------------------------------------------------------
def parse_sse_line(line: Any) -> Optional[Dict[str, Any]]:
    """Parse a ``data:`` line, returning ``None`` for comments/[DONE]."""
    if isinstance(line, bytes):
        text = line.decode("utf-8", errors="replace")
    else:
        text = str(line)
    text = text.strip()
    if not text or text.startswith(":"):
        return None
    if text.startswith("data:"):
        text = text[5:].strip()
    if not text or text == "[DONE]":
        return None
    try:
        value = json.loads(text)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _checked_event(value: Dict[str, Any], retry_after: Optional[int] = None) -> Dict[str, Any]:
    error = error_to_exception(value, retry_after)
    if error:
        raise error
    return value


def _retry_after(response: Any) -> Optional[int]:
    headers = getattr(response, "headers", None) or {}
    value = headers.get("retry-after") or headers.get("Retry-After")
    try:
        seconds = int(float(value)) if value else None
    except (TypeError, ValueError):
        seconds = None
    return seconds if seconds is not None and seconds >= 0 else None


def _accumulate_chat_chunk(payload: Dict[str, Any], state: Dict[str, Any]) -> None:
    if payload.get("id"):
        state["id"] = payload["id"]
    if payload.get("created"):
        state["created"] = payload["created"]
    if payload.get("model"):
        state["model"] = payload["model"]
    if isinstance(payload.get("usage"), dict):
        state["usage"] = payload["usage"]

    choices = payload.get("choices")
    if not isinstance(choices, list):
        return
    for choice in choices:
        if not isinstance(choice, dict):
            continue
        delta = choice.get("delta")
        if not isinstance(delta, dict):
            delta = choice.get("message")
        if not isinstance(delta, dict):
            delta = {}
        content = delta.get("content")
        if isinstance(content, str):
            state["content"] += content
        reasoning = delta.get("reasoning_content") or delta.get("reasoning")
        if isinstance(reasoning, str):
            state["reasoning_content"] += reasoning

        tool_calls = delta.get("tool_calls")
        if isinstance(tool_calls, list):
            for tc in tool_calls:
                if not isinstance(tc, dict):
                    continue
                index = tc.get("index", 0)
                entry = state["tool_calls"].setdefault(
                    index,
                    {
                        "id": tc.get("id") or "",
                        "type": tc.get("type") or "function",
                        "function": {"name": "", "arguments": ""},
                    },
                )
                if tc.get("id"):
                    entry["id"] = tc["id"]
                if tc.get("type"):
                    entry["type"] = tc["type"]
                function = tc.get("function")
                if isinstance(function, dict):
                    if function.get("name"):
                        entry["function"]["name"] += function["name"]
                    if function.get("arguments"):
                        entry["function"]["arguments"] += function["arguments"]
        if choice.get("finish_reason"):
            state["finish_reason"] = choice["finish_reason"]


def collect_chat_completion(response, fallback_model: str) -> Dict[str, Any]:
    state = {
        "id": f"chatcmpl-opencode-{int(time.time())}",
        "created": int(time.time()),
        "model": fallback_model,
        "content": "",
        "reasoning_content": "",
        "tool_calls": {},
        "finish_reason": "stop",
        "usage": None,
    }
    for line in response.iter_lines():
        payload = parse_sse_line(line)
        if payload is None:
            continue
        payload = _checked_event(payload, _retry_after(response))
        _accumulate_chat_chunk(payload, state)

    message: Dict[str, Any] = {"role": "assistant", "content": state["content"]}
    if state["reasoning_content"]:
        message["reasoning_content"] = state["reasoning_content"]
    if state["tool_calls"]:
        message["tool_calls"] = [state["tool_calls"][i] for i in sorted(state["tool_calls"])]

    result = {
        "id": state["id"],
        "object": "chat.completion",
        "created": state["created"],
        "model": state["model"],
        "choices": [
            {
                "index": 0,
                "message": message,
                "finish_reason": state["finish_reason"],
            }
        ],
    }
    if state["usage"]:
        result["usage"] = state["usage"]
    return result


def _accumulate_anthropic_event(payload: Dict[str, Any], state: Dict[str, Any]) -> None:
    event_type = payload.get("type")
    if event_type == "message_start":
        message = payload.get("message")
        if isinstance(message, dict):
            state["message"] = message
            state["content"] = message.get("content") or []
            state["usage"] = message.get("usage") or {}
    elif event_type == "content_block_start":
        index = payload.get("index", 0)
        block = payload.get("content_block")
        if isinstance(block, dict):
            state["blocks"][index] = dict(block)
            state["json_buffers"][index] = ""
    elif event_type == "content_block_delta":
        index = payload.get("index", 0)
        delta = payload.get("delta")
        block = state["blocks"].get(index)
        if not isinstance(delta, dict) or not isinstance(block, dict):
            return
        delta_type = delta.get("type")
        if delta_type == "text_delta":
            block["text"] = block.get("text", "") + delta.get("text", "")
        elif delta_type == "input_json_delta":
            state["json_buffers"][index] += delta.get("partial_json", "")
        elif delta_type == "thinking_delta":
            block["thinking"] = block.get("thinking", "") + delta.get("thinking", "")
        elif delta_type == "signature_delta":
            block["signature"] = block.get("signature", "") + delta.get("signature", "")
    elif event_type == "content_block_stop":
        index = payload.get("index", 0)
        block = state["blocks"].get(index)
        buffer = state["json_buffers"].get(index, "")
        if isinstance(block, dict) and block.get("type") == "tool_use" and buffer:
            try:
                block["input"] = json.loads(buffer)
            except (TypeError, ValueError):
                block["input"] = {}
    elif event_type == "message_delta":
        delta = payload.get("delta")
        if isinstance(delta, dict) and delta.get("stop_reason"):
            state["stop_reason"] = delta["stop_reason"]
        usage = payload.get("usage")
        if isinstance(usage, dict):
            state["usage"].update(usage)
    elif event_type == "message_stop":
        state["done"] = True


def collect_anthropic_message(response, fallback_model: str) -> Dict[str, Any]:
    state: Dict[str, Any] = {
        "message": {},
        "content": [],
        "blocks": {},
        "json_buffers": {},
        "usage": {},
        "stop_reason": None,
        "done": False,
    }
    for line in response.iter_lines():
        payload = parse_sse_line(line)
        if payload is None:
            continue
        payload = _checked_event(payload, _retry_after(response))
        _accumulate_anthropic_event(payload, state)

    base = state["message"]
    content = [
        state["blocks"][index]
        for index in sorted(state["blocks"])
        if isinstance(state["blocks"][index], dict)
    ] or state["content"]
    return {
        "id": base.get("id", f"msg_opencode_{int(time.time())}"),
        "type": "message",
        "role": "assistant",
        "model": base.get("model", fallback_model),
        "content": content,
        "stop_reason": state["stop_reason"] or base.get("stop_reason"),
        "stop_sequence": base.get("stop_sequence"),
        "usage": state["usage"] or base.get("usage") or {},
    }


def collect_responses_api(response, fallback_model: str) -> Dict[str, Any]:
    text = ""
    response_object: Optional[Dict[str, Any]] = None
    output_items: List[Any] = []
    created_at = int(time.time())

    for line in response.iter_lines():
        payload = parse_sse_line(line)
        if payload is None:
            continue
        payload = _checked_event(payload, _retry_after(response))
        event_type = payload.get("type")
        if event_type == "response.output_text.delta":
            text += payload.get("delta", "")
        elif event_type == "response.output_item.done":
            item = payload.get("item")
            if isinstance(item, dict):
                output_items.append(item)
        elif event_type == "response.completed":
            candidate = payload.get("response")
            if isinstance(candidate, dict):
                response_object = candidate

    if response_object:
        return response_object

    result: Dict[str, Any] = {
        "id": f"resp_opencode_{int(time.time())}",
        "object": "response",
        "created_at": created_at,
        "status": "completed",
        "model": fallback_model,
        "output": output_items
        or [
            {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": text}],
            }
        ],
    }
    if text:
        result["output_text"] = text
    return result


# ---------------------------------------------------------------------------
# HTTP/1.1 Upstream Transport Execution
# ---------------------------------------------------------------------------
def create_upstream_session(proxies: Optional[Dict[str, str]] = None) -> Session:
    """Create a fresh HTTP/1.1 session configured with OpenCode-compliant TLS ciphers."""
    session = Session(http_version=CurlHttpVersion.V1_1, proxies=proxies)
    session.curl.setopt(CurlOpt.SSLVERSION, 6)
    session.curl.setopt(CurlOpt.SSL_CIPHER_LIST, TLS12_CIPHERS.encode("utf-8"))
    return session


def upstream_error_response(error: UpstreamAPIError) -> JSONResponse:
    if error.kind == "free_tier":
        status_code = 403
        error_type = "free_tier_restricted"
    elif error.kind == "free_usage_limit":
        status_code = 429
        error_type = "free_usage_limit"
    elif error.kind == "rate_limit":
        status_code = 429
        error_type = "rate_limit"
    else:
        status_code = error.status_code or 502
        error_type = "upstream_error"

    resp_headers = {"X-Upstream-Error": error.kind}
    if error.retry_after is not None:
        resp_headers["Retry-After"] = str(error.retry_after)

    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "message": error.message,
                "type": error_type,
                "code": status_code,
            }
        },
        headers=resp_headers,
    )


def upstream_status_response(response, model_name: str) -> JSONResponse:
    payload = response_payload(
        response,
        default_message=f"Upstream returned HTTP {response.status_code}.",
    )
    if not isinstance(payload, dict):
        payload = {
            "error": {
                "message": f"Upstream returned HTTP {response.status_code}.",
                "type": "upstream_error",
                "code": response.status_code,
            }
        }

    retry_after = response.headers.get("retry-after")
    retry_seconds = None
    try:
        retry_seconds = int(float(retry_after)) if retry_after else None
    except (TypeError, ValueError):
        pass

    error = error_to_exception(payload, retry_seconds)
    if error:
        log.warning(
            "Upstream error for model '%s': kind=%s status=%s message=%s",
            model_name,
            error.kind,
            response.status_code,
            error.message,
        )
        return upstream_error_response(error)

    return JSONResponse(status_code=response.status_code, content=payload)


async def forward_upstream_request(
    endpoint: str,
    target_url: str,
    payload: Dict[str, Any],
    raw_request: Request,
    collector_func=collect_chat_completion,
    client_wants_stream: Optional[bool] = None,
):
    start_time = time.time()
    metrics["total_requests"] += 1

    raw_model = payload.get("model")
    discovered = [m["id"] for m in get_discovered_models()]
    if not raw_model or (discovered and raw_model not in discovered):
        model_name = get_default_model()
    else:
        model_name = raw_model
    payload["model"] = model_name

    if client_wants_stream is None:
        client_wants_stream = bool(payload.get("stream", False))

    prom_requests_total.labels(model=model_name, endpoint=endpoint).inc()

    # Upstream free tier always requires streaming
    payload["stream"] = True
    headers = build_upstream_headers(raw_request, endpoint)
    proxies = get_next_outbound_proxy()

    def _execute_post():
        sess = create_upstream_session(proxies=proxies)
        try:
            resp = sess.post(
                target_url,
                json=payload,
                headers=headers,
                stream=True,
                timeout=STREAM_TIMEOUT,
            )
            return resp, sess
        except Exception:
            sess.close()
            raise

    try:
        response, session = await asyncio.to_thread(_execute_post)
    except Exception as exc:
        metrics["failed_requests"] += 1
        log.error("Failed connecting to upstream %s: %s", target_url, exc)
        return JSONResponse(
            status_code=502,
            content={
                "error": {
                    "message": f"Failed connecting to upstream: {exc}",
                    "type": "upstream_connect_error",
                    "code": 502,
                }
            },
        )

    # Handle HTTP error statuses
    if response.status_code == 429:
        session.close()
        metrics["rate_limited_requests"] += 1
        prom_requests_rate_limited.labels(model=model_name).inc()
        category, retry_seconds, err_payload = classify_upstream_429(response)
        resp_headers = {"X-Rate-Limit-Reason": category}
        if retry_seconds is not None:
            resp_headers["Retry-After"] = str(retry_seconds)
        return JSONResponse(status_code=429, content=err_payload, headers=resp_headers)

    if response.status_code >= 400:
        session.close()
        metrics["failed_requests"] += 1
        return upstream_status_response(response, model_name)

    # Success: record metrics
    metrics["successful_requests"] += 1
    prom_requests_success.labels(model=model_name).inc()

    if client_wants_stream:
        loop = asyncio.get_event_loop()
        line_iter = response.iter_lines()

        def peek_first_line():
            try:
                for line in line_iter:
                    if line:
                        return line
                return None
            except Exception as e:
                return e

        first_line = await loop.run_in_executor(None, peek_first_line)
        if isinstance(first_line, Exception):
            session.close()
            return JSONResponse(
                status_code=502,
                content={"error": {"message": f"Stream error: {first_line}", "code": 502}},
            )

        if first_line:
            raw_text = first_line.decode("utf-8", errors="ignore").strip()
            if raw_text.startswith("{") and ("FreeUsageLimitError" in raw_text or "error" in raw_text):
                session.close()
                try:
                    err_json = json.loads(raw_text)
                    err_exc = error_to_exception(err_json)
                    if err_exc:
                        return upstream_error_response(err_exc)
                except Exception:
                    pass
                return JSONResponse(status_code=429, content={"error": {"message": raw_text}})

        async def stream_with_first():
            prom_active_flows.inc()
            try:
                if first_line:
                    yield first_line + b"\n\n"
                while True:
                    line = await loop.run_in_executor(
                        None, lambda: next(line_iter, None)
                    )
                    if line is None:
                        break
                    if line:
                        yield line + b"\n\n"
                    else:
                        yield b"\n"
            finally:
                prom_active_flows.dec()
                session.close()
                prom_request_duration.labels(
                    model=model_name, endpoint=endpoint
                ).observe(time.time() - start_time)

        return StreamingResponse(
            stream_with_first(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
                "Connection": "keep-alive",
            },
        )

    # Non-streaming request: assemble stream into final JSON
    try:
        assembled = await asyncio.to_thread(collector_func, response, model_name)
    except UpstreamAPIError as exc:
        session.close()
        return upstream_error_response(exc)
    except Exception as exc:
        session.close()
        log.error("Failed assembling response for %s: %s", model_name, exc)
        return JSONResponse(
            status_code=502,
            content={
                "error": {
                    "message": f"Failed assembling response: {exc}",
                    "code": 502,
                }
            },
        )
    finally:
        session.close()

    duration = time.time() - start_time
    prom_request_duration.labels(model=model_name, endpoint=endpoint).observe(duration)

    usage = assembled.get("usage", {})
    prompt_tokens = usage.get("prompt_tokens", 0)
    completion_tokens = usage.get("completion_tokens", 0)
    record_usage(model_name, prompt_tokens, completion_tokens, 0.0)

    return JSONResponse(status_code=200, content=assembled)

"""OpenCode free-tier request compatibility.

The public free models only serve requests that look like they come from the
OpenCode CLI. The gate checks:

* the OpenCode ``User-Agent`` and ``x-opencode-*`` headers,
* a session id with OpenCode's ``ses_`` + descending-timestamp shape,
* an ``<env>`` block with ``Working directory:`` at the start of the system
  prompt,
* a tool named ``read``,
* a streaming request (``stream: true``).

This module builds those request shapes and recognizes the two gate errors so
the proxy can return a real HTTP error instead of forwarding JSON with a 200.
"""

import hashlib
import json
import os
import re
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

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


def opencode_user_agent() -> str:
    return (
        f"opencode/{OPENCODE_CHANNEL}/{OPENCODE_VERSION}/{OPENCODE_CLIENT_NAME}"
    )


def is_valid_session_id(value: Any) -> bool:
    return isinstance(value, str) and bool(SESSION_ID_RE.fullmatch(value))


def generate_session_id(seed: str = "", timestamp_ms: Optional[int] = None) -> str:
    """Build an OpenCode-shaped session id.

    OpenCode stores the low 48 bits of ``~((timestamp * 0x1000) + counter)`` as
    twelve hex characters and appends fourteen alphanumeric characters.
    """
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


def _ensure_read_tool(
    body: Dict[str, Any],
    tool_factory,
    none_choice: Any,
) -> None:
    tools = body.get("tools")
    had_tools = isinstance(tools, list) and bool(tools)
    if isinstance(tools, list):
        if not any(_tool_name(tool) == "read" for tool in tools):
            tools.append(tool_factory())
    else:
        body["tools"] = [tool_factory()]
    if not had_tools and body.get("tool_choice") is None:
        body["tool_choice"] = none_choice


def _apply_chat_shape(body: Dict[str, Any], workspace: Optional[str]) -> None:
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


def _apply_anthropic_shape(body: Dict[str, Any], workspace: Optional[str]) -> None:
    system = body.get("system")
    if isinstance(system, str):
        body["system"] = _prefix_env(system, workspace)
    elif isinstance(system, list):
        text = "".join(
            block.get("text", "")
            for block in system
            if isinstance(block, dict) and isinstance(block.get("text"), str)
        )
        if not _env_present(text):
            block, _ = _env_block(workspace)
            system.insert(0, {"type": "text", "text": block})
    elif system is None:
        block, _ = _env_block(workspace)
        body["system"] = block

    _ensure_read_tool(body, _read_tool_anthropic, {"type": "none"})
    body["stream"] = True


def _apply_responses_shape(body: Dict[str, Any], workspace: Optional[str]) -> None:
    instructions = body.get("instructions")
    if isinstance(instructions, str):
        body["instructions"] = _prefix_env(instructions, workspace)
    elif instructions is None:
        block, _ = _env_block(workspace)
        body["instructions"] = block

    _ensure_read_tool(body, _read_tool_responses, "none")
    body["stream"] = True


def apply_free_request_shape(
    body: Dict[str, Any],
    endpoint: str = "chat",
    workspace: Optional[str] = None,
) -> Dict[str, Any]:
    """Mutate ``body`` so the OpenCode free-tier gate accepts it."""
    if not isinstance(body, dict):
        return body
    if endpoint == "messages":
        _apply_anthropic_shape(body, workspace)
    elif endpoint == "responses":
        _apply_responses_shape(body, workspace)
    else:
        _apply_chat_shape(body, workspace)
    return body


def session_id_from_headers(headers) -> Optional[str]:
    for name in (
        "x-opencode-session",
        "x-opencode-session-id",
        "x-session-affinity",
        "x-session-id",
    ):
        value = headers.get(name)
        if is_valid_session_id(value):
            return value
    return None


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


def _checked_event(value: Dict[str, Any], retry_after: Optional[int]) -> Dict[str, Any]:
    error = error_to_exception(value, retry_after)
    if error:
        raise error
    return value


def _accumulate_chat_chunk(
    payload: Dict[str, Any],
    state: Dict[str, Any],
) -> None:
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
            state["reasoning"] += reasoning
        tool_calls = delta.get("tool_calls")
        if isinstance(tool_calls, list):
            for tool_call in tool_calls:
                if not isinstance(tool_call, dict):
                    continue
                index = tool_call.get("index", 0)
                entry = state["tool_calls"].setdefault(
                    index,
                    {
                        "id": "",
                        "type": "function",
                        "function": {"name": "", "arguments": ""},
                    },
                )
                if tool_call.get("id"):
                    entry["id"] = tool_call["id"]
                function = tool_call.get("function")
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
        "reasoning": "",
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

    message: Dict[str, Any] = {"role": "assistant", "content": state["content"] or None}
    if state["reasoning"]:
        message["reasoning_content"] = state["reasoning"]
    if state["tool_calls"]:
        message["tool_calls"] = [
            state["tool_calls"][index] for index in sorted(state["tool_calls"])
        ]
        if not state["content"]:
            message["content"] = None

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


def _retry_after(response) -> Optional[int]:
    value = None
    try:
        value = response.headers.get("retry-after")
    except Exception:
        pass
    try:
        seconds = int(float(value)) if value else None
    except (TypeError, ValueError):
        seconds = None
    return seconds if seconds is not None and seconds >= 0 else None


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
    result = {
        "id": base.get("id", f"msg_opencode_{int(time.time())}"),
        "type": "message",
        "role": "assistant",
        "model": base.get("model", fallback_model),
        "content": content,
        "stop_reason": state["stop_reason"] or base.get("stop_reason"),
        "stop_sequence": base.get("stop_sequence"),
        "usage": state["usage"] or base.get("usage") or {},
    }
    return result


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

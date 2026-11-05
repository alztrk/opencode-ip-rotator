import json
import logging
from fastapi import APIRouter, HTTPException, Request

from core.config import (
    ANTHROPIC_MESSAGES_URL,
    CHAT_COMPLETIONS_URL,
    RESPONSES_URL,
)
from services.upstream import (
    apply_free_request_shape,
    collect_anthropic_message,
    collect_chat_completion,
    collect_responses_api,
    forward_upstream_request,
)

log = logging.getLogger("routes.chat")
router = APIRouter()


@router.post("/v1/chat/completions")
async def chat_completions(raw_request: Request):
    try:
        payload = await raw_request.json()
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise HTTPException(
            status_code=400, detail="Request body must contain valid JSON"
        ) from exc

    client_wants_stream = bool(payload.get("stream", False))
    payload = apply_free_request_shape(payload, "chat")

    return await forward_upstream_request(
        endpoint="chat_completions",
        target_url=CHAT_COMPLETIONS_URL,
        payload=payload,
        raw_request=raw_request,
        collector_func=collect_chat_completion,
        client_wants_stream=client_wants_stream,
    )


@router.post("/v1/messages")
async def messages(raw_request: Request):
    try:
        payload = await raw_request.json()
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise HTTPException(
            status_code=400, detail="Request body must contain valid JSON"
        ) from exc

    client_wants_stream = bool(payload.get("stream", False))
    payload = apply_free_request_shape(payload, "messages")

    return await forward_upstream_request(
        endpoint="anthropic_messages",
        target_url=ANTHROPIC_MESSAGES_URL,
        payload=payload,
        raw_request=raw_request,
        collector_func=collect_anthropic_message,
        client_wants_stream=client_wants_stream,
    )


@router.post("/v1/responses")
async def responses(raw_request: Request):
    try:
        payload = await raw_request.json()
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise HTTPException(
            status_code=400, detail="Request body must contain valid JSON"
        ) from exc

    client_wants_stream = bool(payload.get("stream", False))
    payload = apply_free_request_shape(payload, "responses")

    return await forward_upstream_request(
        endpoint="responses",
        target_url=RESPONSES_URL,
        payload=payload,
        raw_request=raw_request,
        collector_func=collect_responses_api,
        client_wants_stream=client_wants_stream,
    )

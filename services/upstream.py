import asyncio
import json
import logging
import time
from typing import Any, AsyncGenerator, Dict, Optional, Tuple

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
from opencode_free import (
    UpstreamAPIError,
    collect_anthropic_message,
    collect_chat_completion,
    collect_responses_api,
    error_to_exception,
    opencode_free_headers,
    session_id_from_headers,
)
from rate_limits import classify_upstream_429, response_payload
from services.discovery import get_default_model, get_discovered_models
from services.proxy_pool import get_next_outbound_proxy

log = logging.getLogger("services.upstream")


def create_upstream_session(proxies: Optional[Dict[str, str]] = None) -> Session:
    """Create a fresh HTTP/1.1 session configured with OpenCode-compliant TLS ciphers."""
    session = Session(http_version=CurlHttpVersion.V1_1, proxies=proxies)
    session.curl.setopt(CurlOpt.SSLVERSION, 6)
    session.curl.setopt(CurlOpt.SSL_CIPHER_LIST, TLS12_CIPHERS.encode("utf-8"))
    return session


def build_upstream_headers(raw_request: Request, endpoint: str) -> Dict[str, str]:
    session_id = session_id_from_headers(raw_request.headers)
    project_id = raw_request.headers.get("x-opencode-project")
    headers = opencode_free_headers(session_id, project_id)

    for key, value in raw_request.headers.items():
        lower = key.lower()
        if lower.startswith("x-opencode-") or lower.startswith("x-session-"):
            headers[key] = value

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
            if key.lower().startswith("anthropic-"):
                headers[key] = value

    return headers


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


async def sse_streaming_generator(
    response,
    model_name: str,
    session: Session,
    start_time: float,
    endpoint: str,
) -> AsyncGenerator[bytes, None]:
    loop = asyncio.get_event_loop()
    prom_active_flows.inc()
    line_iter = response.iter_lines()

    def get_next_line():
        try:
            return next(line_iter)
        except StopIteration:
            return None

    try:
        while True:
            line = await loop.run_in_executor(None, get_next_line)
            if line is None:
                break
            if line:
                yield line + b"\n\n"
            else:
                yield b"\n"
    finally:
        prom_active_flows.dec()
        session.close()
        duration = time.time() - start_time
        prom_request_duration.labels(model=model_name, endpoint=endpoint).observe(duration)


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
        # Pre-validate first line to catch in-stream errors (e.g. FreeUsageLimitError in 200 response)
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

    # Track usage if usage dict exists
    usage = assembled.get("usage", {})
    prompt_tokens = usage.get("prompt_tokens", 0)
    completion_tokens = usage.get("completion_tokens", 0)
    record_usage(model_name, prompt_tokens, completion_tokens, 0.0)

    return JSONResponse(status_code=200, content=assembled)

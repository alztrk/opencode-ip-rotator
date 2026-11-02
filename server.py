import asyncio
import json
import logging
import os
import random
import signal
import sqlite3
import threading
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional

import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse, Response
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

from curl_cffi import requests as cffi_requests
from model_catalog import DEFAULT_MODELS_URL, fetch_free_models
from opencode_free import (
    UpstreamAPIError,
    apply_free_request_shape,
    collect_anthropic_message,
    collect_chat_completion,
    collect_responses_api,
    error_to_exception,
    opencode_free_headers,
    parse_sse_line,
    session_id_from_headers,
)
from rate_limits import classify_upstream_429, response_payload
from rotator import flow_lock, active_flows_count, get_public_ip, get_ip_location, rotation_count

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
IP_HISTORY_LIMIT = 20
PAGE_SIZE = 5
BACKOFF_CAP = 30
POLL_ATTEMPTS = 6
WARP_ROTATION_ATTEMPTS = 4
WARP_POST_ROTATION_SLEEP = 3
DEFAULT_PROMPT_TOKENS = 50
DEFAULT_COMPLETION_TOKENS = 100
STREAM_CHUNK_SIZE = 4096
MODEL_DISCOVERY_INTERVAL = 300
DASHBOARD_REFRESH_INTERVAL = 3
STARTUP_TIME = time.time()
ENABLE_HTTP2 = os.environ.get("ENABLE_HTTP2", "false").lower() in ("true", "1", "yes")
STREAM_TIMEOUT = 600
FLOW_LEASE_TTL_SECONDS = int(os.environ.get("FLOW_LEASE_TTL_SECONDS", "90"))
FLOW_LEASE_HEARTBEAT_SECONDS = int(os.environ.get("FLOW_LEASE_HEARTBEAT_SECONDS", "15"))

# -----------------------------------------------------------------------------
# JSON Structured Logging
# -----------------------------------------------------------------------------
class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": self.formatTime(record, self.datefmt or "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0]:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry)

# -----------------------------------------------------------------------------
# Proxy Pool / Custom Proxy List Support
# -----------------------------------------------------------------------------
PROXY_FILE = Path(os.environ.get("PROXY_LIST_FILE", "/app/data/proxies.txt"))
_proxy_pool: List[str] = []
_proxy_index = 0
_proxy_lock = threading.Lock()

def load_proxy_list():
    global _proxy_pool
    proxies = []
    if PROXY_FILE.exists():
        try:
            with open(PROXY_FILE, "r", encoding="utf-8") as f:
                lines = [line.strip() for line in f if line.strip() and not line.startswith("#")]
                proxies.extend(lines)
        except Exception as e:
            log.error(f"Error reading proxies.txt: {e}")
    env_proxies = os.environ.get("PROXY_LIST", "").strip()
    if env_proxies:
        proxies.extend([p.strip() for p in env_proxies.split(",") if p.strip()])
    _proxy_pool = list(dict.fromkeys(proxies))
    if _proxy_pool:
        log.info(f"Loaded {len(_proxy_pool)} custom proxies into pool.")

def get_next_outbound_proxy() -> Optional[Dict[str, str]]:
    global _proxy_index
    with _proxy_lock:
        if _proxy_pool:
            proxy_url = _proxy_pool[_proxy_index % len(_proxy_pool)]
            _proxy_index += 1
            return {"http": proxy_url, "https": proxy_url}
        custom_proxy = os.environ.get("CUSTOM_OUTBOUND_PROXY", "").strip()
        if custom_proxy:
            return {"http": custom_proxy, "https": custom_proxy}

# -----------------------------------------------------------------------------
# SQLite — WAL mode + retry for concurrent safety
# -----------------------------------------------------------------------------
DB_FILE = Path(os.environ.get("METRICS_DB_PATH", "/app/data/metrics.db"))
_db_lock = threading.Lock()

def _get_conn():
    conn = sqlite3.connect(str(DB_FILE), timeout=5)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def _db_execute(statement: str, params=()):
    for attempt in range(3):
        try:
            with _db_lock:
                conn = _get_conn()
                try:
                    cursor = conn.cursor()
                    cursor.execute(statement, params)
                    conn.commit()
                    return cursor
                finally:
                    conn.close()
        except sqlite3.OperationalError as e:
            if "busy" in str(e).lower() and attempt < 2:
                time.sleep(0.1 * (attempt + 1))
                continue
            raise

def _db_fetchall(statement: str, params=()) -> list:
    for attempt in range(3):
        try:
            with _db_lock:
                conn = _get_conn()
                try:
                    cursor = conn.cursor()
                    cursor.execute(statement, params)
                    return cursor.fetchall()
                finally:
                    conn.close()
        except sqlite3.OperationalError as e:
            if "busy" in str(e).lower() and attempt < 2:
                time.sleep(0.1 * (attempt + 1))
                continue
            raise

def init_db():
    DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    _db_execute("""
        CREATE TABLE IF NOT EXISTS model_usage (
            model_name TEXT PRIMARY KEY,
            requests INTEGER DEFAULT 0,
            prompt_tokens INTEGER DEFAULT 0,
            completion_tokens INTEGER DEFAULT 0,
            total_tokens INTEGER DEFAULT 0,
            estimated_cost_usd REAL DEFAULT 0.0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    _db_execute("""
        CREATE TABLE IF NOT EXISTS ip_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ip TEXT,
            country TEXT,
            flag TEXT,
            timestamp TEXT,
            reason TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    _db_execute("""
        CREATE TABLE IF NOT EXISTS warp_quality (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            success INTEGER,
            latency_ms REAL,
            old_ip TEXT,
            new_ip TEXT
        )
    """)
    _db_execute("""
        CREATE TABLE IF NOT EXISTS active_flow_leases (
            lease_id TEXT PRIMARY KEY,
            expires_at REAL NOT NULL
        )
    """)


def acquire_flow_lease() -> str:
    lease_id = uuid.uuid4().hex
    touch_flow_lease(lease_id)
    return lease_id


def touch_flow_lease(lease_id: str) -> None:
    _db_execute(
        "INSERT OR REPLACE INTO active_flow_leases (lease_id, expires_at) VALUES (?, ?)",
        (lease_id, time.time() + FLOW_LEASE_TTL_SECONDS),
    )


def release_flow_lease(lease_id: str) -> None:
    _db_execute("DELETE FROM active_flow_leases WHERE lease_id = ?", (lease_id,))

def log_ip_rotation_to_db(ip: str, country: str, flag: str, timestamp: str, reason: str):
    try:
        _db_execute(
            "INSERT INTO ip_history (ip, country, flag, timestamp, reason) VALUES (?, ?, ?, ?, ?)",
            (ip, country, flag, timestamp, reason)
        )
    except Exception as e:
        log.error(f"Failed to log IP rotation to DB: {e}")

def load_ip_history_from_db() -> List[Dict[str, Any]]:
    if not DB_FILE.exists():
        return []
    try:
        rows = _db_fetchall(
            "SELECT ip, country, flag, timestamp, reason FROM ip_history ORDER BY id DESC LIMIT ?",
            (IP_HISTORY_LIMIT,)
        )
        history = []
        for r in reversed(rows):
            history.append({
                "ip": r[0], "country": r[1], "flag": r[2],
                "timestamp": r[3], "reason": r[4]
            })
        return history
    except Exception as e:
        log.error(f"Error loading IP history from DB: {e}")
        return []

def load_metrics_from_db() -> Dict[str, Dict[str, Any]]:
    if not DB_FILE.exists():
        return {}
    try:
        rows = _db_fetchall(
            "SELECT model_name, requests, prompt_tokens, completion_tokens, total_tokens, estimated_cost_usd FROM model_usage"
        )
        stats = {}
        for r in rows:
            stats[r[0]] = {
                "requests": r[1], "prompt_tokens": r[2],
                "completion_tokens": r[3], "total_tokens": r[4],
                "estimated_cost_usd": r[5]
            }
        return stats
    except Exception as e:
        log.error(f"Error loading metrics from DB: {e}")
        return {}

# -----------------------------------------------------------------------------
# Prometheus Metrics
# -----------------------------------------------------------------------------
prom_requests_total = Counter("proxy_requests_total", "Total proxied requests", ["model", "endpoint"])
prom_requests_success = Counter("proxy_requests_success", "Successful proxied requests", ["model"])
prom_requests_rate_limited = Counter("proxy_requests_rate_limited", "Rate-limited requests", ["model"])
prom_rotation_count = Counter("proxy_rotations_total", "Total WARP rotations")
prom_active_flows = Gauge("proxy_active_flows", "Currently active streaming flows")
prom_request_duration = Histogram("proxy_request_duration_seconds", "Request duration", ["model", "endpoint"],
                                   buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0))
prom_warp_health = Gauge("proxy_warp_health", "WARP health (1=healthy, 0=unhealthy)")

# -----------------------------------------------------------------------------
# curl_cffi Session Pool
# -----------------------------------------------------------------------------
_session_pool: Dict[str, "SessionType"] = {}
_session_pool_lock = threading.Lock()
SessionType = None  # resolved at first use

def _get_session(endpoint: str):
    global SessionType
    if SessionType is None:
        from curl_cffi.requests import Session as SessionType
    with _session_pool_lock:
        if endpoint not in _session_pool:
            kwargs = {}
            if not ENABLE_HTTP2:
                from curl_cffi import CurlHttpVersion
                kwargs["http_version"] = CurlHttpVersion.V1_1
            _session_pool[endpoint] = SessionType(**kwargs)
        return _session_pool[endpoint]

def create_fresh_session(is_stream: bool):
    global SessionType
    if SessionType is None:
        from curl_cffi.requests import Session as SessionType
    kwargs = {}
    if not ENABLE_HTTP2:
        from curl_cffi import CurlHttpVersion
        kwargs["http_version"] = CurlHttpVersion.V1_1
    return SessionType(**kwargs)

def _close_all_sessions():
    with _session_pool_lock:
        for ep, sess in _session_pool.items():
            try:
                sess.close()
            except Exception:
                pass
        _session_pool.clear()

_discovery_stop = threading.Event()

# -----------------------------------------------------------------------------
# Request Queue (drains during rotation)
# -----------------------------------------------------------------------------
_rotation_in_progress = threading.Event()
_request_drain_event = asyncio.Event()
_request_drain_event.set()

async def wait_for_rotation_drain():
    if _rotation_in_progress.is_set():
        await asyncio.wait_for(_request_drain_event.wait(), timeout=15)

def signal_rotation_start():
    _rotation_in_progress.set()
    _request_drain_event.clear()

def signal_rotation_done():
    _rotation_in_progress.clear()
    _request_drain_event.set()

# -----------------------------------------------------------------------------
# Dual-WARP (active/passive tracking)
# -----------------------------------------------------------------------------
_dual_warp = {
    "active_ip": None,
    "standby_ip": None,
    "active_registration": "primary",
}
_dual_warp_lock = threading.Lock()

def swap_warp_registration():
    with _dual_warp_lock:
        _dual_warp["active_registration"] = (
            "standby" if _dual_warp["active_registration"] == "primary" else "primary"
        )
        return _dual_warp["active_registration"]

# -----------------------------------------------------------------------------
# Model pricing reference (USD per 1M tokens)
# -----------------------------------------------------------------------------
MODEL_PRICING = {
    "deepseek-v4-flash-free": {"input_per_1m": 0.15, "output_per_1m": 0.60},
    "mimo-v2.5-free": {"input_per_1m": 0.20, "output_per_1m": 0.80},
    "qwen3.6-plus-free": {"input_per_1m": 0.40, "output_per_1m": 1.20},
    "minimax-m3-free": {"input_per_1m": 0.30, "output_per_1m": 1.00},
    "nemotron-3-ultra-free": {"input_per_1m": 0.25, "output_per_1m": 0.90},
    "ling-3.0-flash-free": {"input_per_1m": 0.15, "output_per_1m": 0.50},
    "laguna-s-2.1-free": {"input_per_1m": 0.20, "output_per_1m": 0.70},
}

_model_usage_lock = threading.Lock()

def track_token_usage(model_name: str, prompt_tokens: int = 0, completion_tokens: int = 0):
    global model_usage_stats
    pricing = MODEL_PRICING.get(model_name, {"input_per_1m": 0.20, "output_per_1m": 0.80})
    prompt_cost = (prompt_tokens / 1_000_000) * pricing["input_per_1m"]
    completion_cost = (completion_tokens / 1_000_000) * pricing["output_per_1m"]
    cost = prompt_cost + completion_cost

    with _model_usage_lock:
        if model_name not in model_usage_stats:
            model_usage_stats[model_name] = {
                "requests": 0, "prompt_tokens": 0, "completion_tokens": 0,
                "total_tokens": 0, "estimated_cost_usd": 0.0
            }
        model_usage_stats[model_name]["requests"] += 1
        model_usage_stats[model_name]["prompt_tokens"] += prompt_tokens
        model_usage_stats[model_name]["completion_tokens"] += completion_tokens
        model_usage_stats[model_name]["total_tokens"] += (prompt_tokens + completion_tokens)
        model_usage_stats[model_name]["estimated_cost_usd"] += cost

    try:
        _db_execute("""
            INSERT INTO model_usage (model_name, requests, prompt_tokens, completion_tokens, total_tokens, estimated_cost_usd)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(model_name) DO UPDATE SET
                requests = requests + excluded.requests,
                prompt_tokens = prompt_tokens + excluded.prompt_tokens,
                completion_tokens = completion_tokens + excluded.completion_tokens,
                total_tokens = total_tokens + excluded.total_tokens,
                estimated_cost_usd = estimated_cost_usd + excluded.estimated_cost_usd,
                updated_at = CURRENT_TIMESTAMP
        """, (model_name, 1, prompt_tokens, completion_tokens, prompt_tokens + completion_tokens, cost))
    except Exception as e:
        log.error(f"Failed to persist metrics to SQLite: {e}")

# -----------------------------------------------------------------------------
# WARP Quality Metrics
# -----------------------------------------------------------------------------
warp_quality_stats = {
    "total_attempts": 0, "successful_rotations": 0, "failed_rotations": 0,
    "last_latency_ms": 0.0, "avg_latency_ms": 0.0
}
_warp_quality_lock = threading.Lock()

def record_warp_rotation(success: bool, latency_ms: float = 0, old_ip: str = "", new_ip: str = ""):
    with _warp_quality_lock:
        warp_quality_stats["total_attempts"] += 1
        if success:
            warp_quality_stats["successful_rotations"] += 1
            warp_quality_stats["last_latency_ms"] = latency_ms
            n = warp_quality_stats["successful_rotations"]
            warp_quality_stats["avg_latency_ms"] = (
                (warp_quality_stats["avg_latency_ms"] * (n - 1) + latency_ms) / n
            )
        else:
            warp_quality_stats["failed_rotations"] += 1
    try:
        _db_execute(
            "INSERT INTO warp_quality (timestamp, success, latency_ms, old_ip, new_ip) VALUES (?, ?, ?, ?, ?)",
            (time.strftime("%Y-%m-%d %H:%M:%S"), 1 if success else 0, latency_ms, old_ip, new_ip)
        )
    except Exception:
        pass

# -----------------------------------------------------------------------------
# Backoff helper
# -----------------------------------------------------------------------------
def compute_backoff_delay(attempt: int, base: float = 1.0, cap: int = BACKOFF_CAP) -> float:
    return min(base * (2 ** (attempt - 1)), cap) + random.uniform(0.5, 1.5)

# -----------------------------------------------------------------------------
# Jinja2 Templates
# -----------------------------------------------------------------------------
_templates = Jinja2Templates(directory=Path(__file__).parent / "templates")

# -----------------------------------------------------------------------------
model_usage_stats: Dict[str, Dict[str, float]] = {}

# Configuration & Dynamic Discovery
# -----------------------------------------------------------------------------
PORT = int(os.environ.get("OPENCODE_ZEN_PORT", "8000"))
HOST = os.environ.get("OPENCODE_ZEN_HOST", "127.0.0.1")
TARGET_ZEN_BASE = os.environ.get("OPENCODE_ZEN_TARGET_BASE", "https://opencode.ai/zen/v1")
TARGET_ZEN_URL = f"{TARGET_ZEN_BASE}/chat/completions"
TARGET_ZEN_ANTHROPIC_URL = f"{TARGET_ZEN_BASE}/messages"
TARGET_ZEN_RESPONSES_URL = f"{TARGET_ZEN_BASE}/responses"
MODEL_CATALOG_URL = os.environ.get("OPENCODE_MODELS_URL", DEFAULT_MODELS_URL)
DEFAULT_MODEL_OVERRIDE = os.environ.get("OPENCODE_DEFAULT_MODEL", "").strip()

MIN_ATTEMPTS_BEFORE_RATE_LIMIT = 3
MAX_IP_ROTATIONS_BEFORE_RATE_LIMIT = 1
MAX_RETRIES_ON_429 = max(
    MIN_ATTEMPTS_BEFORE_RATE_LIMIT,
    int(os.environ.get("MAX_RETRIES_ON_429", "8")),
)
INITIAL_BACKOFF = float(os.environ.get("INITIAL_BACKOFF", "1"))
WARP_ROTATOR_URL = os.environ.get("WARP_ROTATOR_URL", "http://127.0.0.1:8001").rstrip("/")
CORS_ALLOW_ORIGINS = [
    origin.strip()
    for origin in os.environ.get("CORS_ALLOW_ORIGINS", "http://127.0.0.1:8000,http://localhost:8000").split(",")
    if origin.strip()
]

metrics = {
    "total_requests": 0,
    "successful_requests": 0,
    "rate_limited_requests": 0,
    "fallback_triggered": 0,
    "discovered_models_count": 0,
    "start_time": time.time()
}

discovered_models: List[Dict[str, Any]] = []
_discovery_lock = threading.Lock()


def get_default_model() -> str:
    """Pick a model for clients that omit the field.

    The environment override wins, then the first live free model. The final
    fallback exists only for the short window before the first discovery run.
    """
    if DEFAULT_MODEL_OVERRIDE:
        return DEFAULT_MODEL_OVERRIDE
    with _discovery_lock:
        if discovered_models:
            return discovered_models[0]["id"]
    return "big-pickle"

LOG_FORMAT = os.environ.get("LOG_FORMAT", "text").lower()
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_LEVEL_VALUE = getattr(logging, LOG_LEVEL, logging.INFO)

if LOG_FORMAT == "json":
    _handler = logging.StreamHandler()
    _handler.setFormatter(JSONFormatter())
    logging.basicConfig(level=LOG_LEVEL_VALUE, handlers=[_handler], force=True)
else:
    logging.basicConfig(
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        level=LOG_LEVEL_VALUE,
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )
log = logging.getLogger("zen_server")


def refresh_models_once() -> bool:
    """Fetch the catalog once, keeping the last-known-good list on failure."""
    global discovered_models
    try:
        new_models = fetch_free_models(MODEL_CATALOG_URL, timeout=10)
    except Exception as e:
        log.warning(
            "Auto-Discovery failed (%s); keeping last-known-good list (%s).",
            e,
            len(discovered_models),
        )
        return False

    if not new_models:
        log.warning(
            "Auto-Discovery returned no free models from %s; keeping last-known-good list (%s).",
            MODEL_CATALOG_URL,
            len(discovered_models),
        )
        return False

    with _discovery_lock:
        discovered_models = new_models
        metrics["discovered_models_count"] = len(discovered_models)
    log.info(
        "Auto-Discovery refreshed: %s free model(s) from %s.",
        len(new_models),
        MODEL_CATALOG_URL,
    )
    return True


@asynccontextmanager
async def lifespan(application: FastAPI):
    global model_usage_stats
    init_db()
    model_usage_stats = load_metrics_from_db()
    _discovery_stop.clear()
    await asyncio.to_thread(refresh_models_once)
    threading.Thread(target=discover_models_task, daemon=True).start()
    yield
    _close_all_sessions()
    _discovery_stop.set()

app = FastAPI(title="OpenCode Zen v3.0 Ultra Resilient Proxy", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

def discover_models_task():
    while not _discovery_stop.is_set():
        refresh_models_once()
        _discovery_stop.wait(MODEL_DISCOVERY_INTERVAL)

class FlowContext:
    def __enter__(self):
        global active_flows_count
        with flow_lock:
            active_flows_count += 1
            prom_active_flows.set(active_flows_count)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global active_flows_count
        with flow_lock:
            active_flows_count = max(0, active_flows_count - 1)
            prom_active_flows.set(active_flows_count)

class ChatMessage(BaseModel):
    role: str
    content: str

class ChatCompletionRequest(BaseModel):
    model: Optional[str] = Field(default=None)
    messages: List[ChatMessage]
    stream: Optional[bool] = False
    temperature: Optional[float] = 0.7
    max_tokens: Optional[int] = None

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


def response_retry_after(response) -> Optional[int]:
    value = response.headers.get("retry-after")
    try:
        seconds = int(float(value)) if value else None
    except (TypeError, ValueError):
        seconds = None
    return seconds if seconds is not None and seconds >= 0 else None


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

    headers = {"X-Upstream-Error": error.kind}
    if error.retry_after is not None:
        headers["Retry-After"] = str(error.retry_after)
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "message": error.message,
                "type": error_type,
                "code": status_code,
            }
        },
        headers=headers,
    )


def upstream_status_response(response, model_name: str) -> JSONResponse:
    """Propagate an upstream error status instead of returning it as HTTP 200."""
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

    error = error_to_exception(payload, response_retry_after(response))
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


SAFE_UPSTREAM_HEADERS = {
    "content-type",
    "retry-after",
    "x-request-id",
    "x-ratelimit-limit",
    "x-ratelimit-remaining",
    "x-ratelimit-reset",
    "cf-ray",
}
SENSITIVE_LOG_KEYS = {"authorization", "api_key", "apikey", "token", "password", "secret"}


def redact_for_log(value):
    if isinstance(value, dict):
        return {
            key: "[redacted]"
            if any(marker in key.lower().replace("-", "_") for marker in SENSITIVE_LOG_KEYS)
            else redact_for_log(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact_for_log(item) for item in value]
    if isinstance(value, str) and len(value) > 1000:
        return value[:1000] + "...[truncated]"
    return value


def log_upstream_response(response, model_name: str, endpoint: str, attempt: int, uses_proxy: bool) -> None:
    headers = {
        key: value
        for key, value in response.headers.items()
        if key.lower() in SAFE_UPSTREAM_HEADERS
    }
    log.debug(
        "Upstream response model=%s endpoint=%s attempt=%s status=%s uses_proxy=%s headers=%s",
        model_name,
        endpoint,
        attempt,
        response.status_code,
        uses_proxy,
        headers,
    )


def rate_limit_response(response, model_name: str) -> JSONResponse:
    category, retry_seconds, payload = classify_upstream_429(response)
    headers = {"X-Rate-Limit-Reason": category}
    if retry_seconds is not None:
        headers["Retry-After"] = str(retry_seconds)
    log.warning(
        "Upstream 429 for model '%s' classified as %s (retry_after=%s); headers=%s payload=%s",
        model_name,
        category,
        retry_seconds,
        {key: value for key, value in response.headers.items() if key.lower() in SAFE_UPSTREAM_HEADERS},
        redact_for_log(payload),
    )
    if isinstance(payload, dict) and isinstance(payload.get("error"), dict):
        payload["error"]["type"] = category
    return JSONResponse(status_code=429, content=payload, headers=headers)


def stream_rate_limit_response(model_name: str) -> JSONResponse:
    return JSONResponse(
        status_code=429,
        content={
            "error": {
                "message": f"Stream unavailable for model '{model_name}' after retries and IP rotation.",
                "type": "rate_limit",
                "code": 429,
            }
        },
        headers={"Retry-After": "10", "X-Rate-Limit-Reason": "rate_limit"},
    )


def rotate_egress(reason: str) -> tuple[bool, Optional[str]]:
    """Request rotation from the service that owns the shared WARP namespace."""
    try:
        response = cffi_requests.post(f"{WARP_ROTATOR_URL}/rotate", timeout=35)
        data = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
        if response.status_code == 200 and data.get("status") == "success":
            return True, data.get("verified_ip")
        return False, None
    except Exception as exc:
        log.warning("Rotator request failed: %s", exc)
        return False, None

class EmptyStreamError(Exception):
    """Raised when upstream returns an empty or truncated stream without valid content/tool calls."""
    pass


async def prepare_stream_response(response, model_name: str, session=None, done_marker=b"\ndata: [DONE]\n\n"):
    """Validate the first stream chunk before committing the HTTP response."""
    stream = stream_response(response, model_name, session=session, done_marker=done_marker)
    try:
        first_chunk = await stream.__anext__()
    except Exception:
        await stream.aclose()
        raise

    async def replay_stream():
        try:
            yield first_chunk
            async for chunk in stream:
                yield chunk
        finally:
            await stream.aclose()

    return replay_stream()

async def stream_response(response, model_name: str, session=None, done_marker=b"\ndata: [DONE]\n\n") -> AsyncGenerator[bytes, None]:
    loop = asyncio.get_event_loop()
    global active_flows_count

    with flow_lock:
        active_flows_count += 1
        prom_active_flows.set(active_flows_count)
    lease_id = await asyncio.to_thread(acquire_flow_lease)

    async def keep_flow_lease_alive():
        try:
            while True:
                await asyncio.sleep(FLOW_LEASE_HEARTBEAT_SECONDS)
                await asyncio.to_thread(touch_flow_lease, lease_id)
        except asyncio.CancelledError:
            return

    lease_heartbeat = asyncio.create_task(keep_flow_lease_alive())

    chunk_count = 0
    last_raw_line = ""
    buffered_lines = []
    has_meaningful_content = False

    try:
        def get_next_line(iter_lines):
            try:
                return next(iter_lines)
            except StopIteration:
                return "STOP_ITERATION"
            except Exception as exc:
                log.error(f"[STREAM DEBUG] Upstream socket/connection error for '{model_name}': {type(exc).__name__}: {exc}")
                return "SOCKET_ERROR"

        line_iter = response.iter_lines()

        # Step 1: Buffer up to 10 initial lines or until we confirm non-empty content
        while len(buffered_lines) < 10:
            item = await loop.run_in_executor(None, get_next_line, line_iter)
            if item in ("STOP_ITERATION", "SOCKET_ERROR"):
                break

            line = item
            if line:
                raw_text = line.decode("utf-8", errors="ignore").strip()
                if raw_text and not raw_text.startswith(":"):
                    buffered_lines.append(line)
                    if "content" in raw_text or "tool_calls" in raw_text or "reasoning_content" in raw_text:
                        # Check if it's not just an empty choices array
                        if '"choices":[]' not in raw_text.replace(" ", ""):
                            has_meaningful_content = True
                            break

        # Surface gate/quota errors that arrive inside an HTTP 200 stream.
        for buffered_line in buffered_lines:
            event = parse_sse_line(buffered_line)
            if event:
                api_error = error_to_exception(
                    event, response_retry_after(response)
                )
                if api_error:
                    raise api_error

        # If stream ended prematurely without producing any meaningful content or tool calls
        if not has_meaningful_content and len(buffered_lines) < 4:
            all_buffered = "".join([l.decode("utf-8", errors="ignore") for l in buffered_lines])
            if '"choices":[]' in all_buffered.replace(" ", "") or len(buffered_lines) == 0:
                log.warning(f"[STREAM RECOVERY] Upstream returned empty/truncated stream for '{model_name}'. Triggering IP rotation & raising EmptyStreamError.")
                raise EmptyStreamError("Upstream returned empty response stream")

        # Yield buffered initial lines
        for b_line in buffered_lines:
            chunk_count += 1
            yield b_line + b"\n"

        # Step 2: Continue streaming remaining lines
        while True:
            item = await loop.run_in_executor(None, get_next_line, line_iter)
            if item == "STOP_ITERATION":
                log.info(f"[STREAM DEBUG] Upstream reached natural StopIteration for '{model_name}'. Total lines: {chunk_count}")
                break
            if item == "SOCKET_ERROR":
                log.warning(f"[STREAM DEBUG] Upstream connection aborted via socket error for '{model_name}'. Lines sent: {chunk_count}")
                break

            line = item
            if line:
                chunk_count += 1
                try:
                    last_raw_line = line.decode("utf-8", errors="ignore")
                except Exception:
                    pass
                yield line + b"\n"
            else:
                yield b"\n"

        log.info(f"Streaming completed successfully for model '{model_name}' ({chunk_count} lines sent).")
        if done_marker:
            yield done_marker
    except EmptyStreamError:
        raise
    except GeneratorExit:
        log.warning(f"[STREAM DEBUG] Client (OpenCode) explicitly closed/aborted SSE connection prematurely for '{model_name}' after {chunk_count} lines.")
    except Exception as e:
        log.error(f"Stream exception caught for model '{model_name}': {type(e).__name__}: {e}", exc_info=True)
        yield b"\ndata: [DONE]\n\n"
    finally:
        lease_heartbeat.cancel()
        await asyncio.gather(lease_heartbeat, return_exceptions=True)
        await asyncio.to_thread(release_flow_lease, lease_id)
        with flow_lock:
            active_flows_count = max(0, active_flows_count - 1)
            prom_active_flows.set(active_flows_count)
        if session:
            try:
                session.close()
            except Exception:
                pass

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    return _templates.TemplateResponse(request=request, name="dashboard.html")

@app.get("/metrics-prometheus")
async def metrics_prometheus():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/api/rotate")
async def manual_rotate():
    signal_rotation_start()
    try:
        started = time.monotonic()
        result, verified_ip = await asyncio.to_thread(rotate_egress, "Manual API trigger")
        record_warp_rotation(result, (time.monotonic() - started) * 1000, new_ip=verified_ip or "")
        if result:
            swap_warp_registration()
            return {"status": "success", "verified_ip": verified_ip}
        raise HTTPException(status_code=503, detail="WARP rotator did not complete the requested rotation")
    finally:
        signal_rotation_done()

@app.get("/metrics")
async def get_metrics():
    uptime = int(time.time() - metrics["start_time"])

    # Always ensure full historical stats from SQLite DB are included
    db_usage = load_metrics_from_db()
    for m_name, m_data in db_usage.items():
        if m_name not in model_usage_stats:
            model_usage_stats[m_name] = m_data
        else:
            # Sync highest values or keep memory in sync with DB
            model_usage_stats[m_name]["requests"] = max(model_usage_stats[m_name]["requests"], m_data["requests"])
            model_usage_stats[m_name]["prompt_tokens"] = max(model_usage_stats[m_name]["prompt_tokens"], m_data["prompt_tokens"])
            model_usage_stats[m_name]["completion_tokens"] = max(model_usage_stats[m_name]["completion_tokens"], m_data["completion_tokens"])
            model_usage_stats[m_name]["total_tokens"] = max(model_usage_stats[m_name]["total_tokens"], m_data["total_tokens"])
            model_usage_stats[m_name]["estimated_cost_usd"] = max(model_usage_stats[m_name]["estimated_cost_usd"], m_data["estimated_cost_usd"])

    # Fetch live data from warp-rotator microservice (offloaded to executor — blocking call)
    rotator_ip = None
    rotator_location = None
    rotator_rotations = rotation_count
    rotator_history = []

    def fetch_rotator_status():
        try:
            r = cffi_requests.get(f"{WARP_ROTATOR_URL}/status", impersonate="chrome124", timeout=4)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            log.debug(f"warp-rotator status fetch error: {e}")
        return None

    loop = asyncio.get_event_loop()
    rdata = await loop.run_in_executor(None, fetch_rotator_status)

    if rdata:
        rotator_ip = rdata.get("current_ip")
        rotator_rotations = rdata.get("rotations", rotation_count)
        rotator_history = rdata.get("history", [])
        if rotator_ip:
            def fetch_location():
                return get_ip_location(rotator_ip)
            rotator_location = await loop.run_in_executor(None, fetch_location)

    # Fallback: local IP lookup
    if not rotator_ip:
        def fetch_local_ip():
            ip = get_public_ip()
            loc = get_ip_location(ip) if ip else {"country": "Unknown", "flag": "🌐"}
            return ip, loc
        rotator_ip, rotator_location = await loop.run_in_executor(None, fetch_local_ip)

    # Fallback: SQLite history
    if not rotator_history:
        rotator_history = load_ip_history_from_db()

    if not rotator_location:
        rotator_location = {"country": "Unknown", "flag": "🌐"}

    return {
        "uptime_seconds": uptime,
        "verified_public_ip": rotator_ip,
        "egress_verification_scope": "shared proxy and WARP network namespace",
        "location": rotator_location,
        "total_rotations": rotator_rotations,
        "metrics": metrics,
        "active_flows": active_flows_count,
        "discovered_models": discovered_models,
        "model_usage": model_usage_stats,
        "ip_history": rotator_history,
        "warp_quality": dict(warp_quality_stats),
        "dual_warp": dict(_dual_warp),
        "rotation_in_progress": _rotation_in_progress.is_set()
    }

@app.get("/health")
async def health():
    ip = get_public_ip()
    prom_warp_health.set(1 if ip and ip != "Disconnected" else 0)
    db_ok = False
    try:
        _db_execute("SELECT 1")
        db_ok = True
    except Exception:
        pass
    return {
        "status": "healthy" if db_ok else "degraded",
        "database": "connected" if db_ok else "unreachable",
        "uptime_seconds": int(time.time() - metrics["start_time"]),
        "active_flows": active_flows_count,
        "total_rotations": rotation_count,
        "warp_quality": dict(warp_quality_stats),
    }

@app.get("/v1/models")
async def list_models():
    with _discovery_lock:
        models = list(discovered_models)
    return {
        "object": "list",
        "data": [
            {
                "id": m["id"],
                "object": "model",
                "created": m.get("created") or int(STARTUP_TIME),
                "owned_by": m.get("owned_by") or "opencode",
                "name": m.get("name") or m["id"],
            }
            for m in models
        ]
    }

@app.post("/v1/chat/completions")
async def chat_completions(raw_request: Request):
    metrics["total_requests"] += 1
    prom_requests_total.labels(model="chat", endpoint="chat_completions").inc()
    await wait_for_rotation_drain()

    start_time = time.time()
    try:
        payload = await raw_request.json()
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise HTTPException(status_code=400, detail="Request body must contain valid JSON") from exc

    current_model = payload.get("model") or get_default_model()
    client_stream = bool(payload.get("stream", False))
    payload = apply_free_request_shape(payload, "chat")
    log.info(f"Received request for model '{current_model}' (Stream: {client_stream} | Has Tools: {'tools' in payload})")

    headers = build_upstream_headers(raw_request, "chat")

    ip_rotation_count = 0
    for attempt in range(1, MAX_RETRIES_ON_429 + 1):
        try:
            await asyncio.sleep(random.uniform(0.1, 0.3))
            proxies = get_next_outbound_proxy()
            session = create_fresh_session(True) if client_stream else _get_session("chat")
            response = session.post(
                TARGET_ZEN_URL,
                json=payload,
                headers=headers,
                impersonate="chrome124",
                stream=True,
                proxies=proxies,
                timeout=STREAM_TIMEOUT
            )
            log_upstream_response(response, current_model, "chat_completions", attempt, proxies is not None)

            if response.status_code == 429:
                metrics["rate_limited_requests"] += 1
                prom_requests_rate_limited.labels(model=current_model).inc()
                if attempt < MIN_ATTEMPTS_BEFORE_RATE_LIMIT:
                    delay = compute_backoff_delay(attempt, INITIAL_BACKOFF)
                    log.warning(
                        "Upstream 429 for '%s'; retrying before returning rate_limit (%s/%s) in %.2fs.",
                        current_model,
                        attempt,
                        MIN_ATTEMPTS_BEFORE_RATE_LIMIT,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                if ip_rotation_count < MAX_IP_ROTATIONS_BEFORE_RATE_LIMIT:
                    ip_rotation_count += 1
                    rotated, verified_ip = await asyncio.to_thread(rotate_egress, "upstream rate limit")
                    if rotated:
                        log.warning("Retrying '%s' after IP rotation to %s.", current_model, verified_ip or "unknown")
                        continue
                    log.error("IP rotation failed for '%s' after upstream rate limits.", current_model)
                return rate_limit_response(response, current_model)

            if response.status_code >= 500:
                delay = compute_backoff_delay(attempt, INITIAL_BACKOFF)
                log.warning("Upstream HTTP %s for '%s'; retrying without egress rotation in %.2fs.", response.status_code, current_model, delay)
                await asyncio.sleep(delay)
                continue

            if response.status_code >= 400:
                return upstream_status_response(response, current_model)

            metrics["successful_requests"] += 1
            prom_requests_success.labels(model=current_model).inc()
            prom_request_duration.labels(model=current_model, endpoint="chat_completions").observe(time.time() - start_time)

            if client_stream:
                try:
                    # Pre-verify that the response is not an empty stream before committing to StreamingResponse
                    stream_gen = await prepare_stream_response(response, current_model, session=session)
                    metrics["successful_requests"] += 1
                    prom_requests_success.labels(model=current_model).inc()
                    prom_request_duration.labels(model=current_model, endpoint="chat_completions").observe(time.time() - start_time)
                    track_token_usage(current_model, prompt_tokens=100, completion_tokens=150)
                    return StreamingResponse(
                        stream_gen,
                        media_type="text/event-stream",
                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"}
                    )
                except UpstreamAPIError as exc:
                    return upstream_error_response(exc)
                except EmptyStreamError:
                    if attempt >= MIN_ATTEMPTS_BEFORE_RATE_LIMIT and ip_rotation_count < MAX_IP_ROTATIONS_BEFORE_RATE_LIMIT:
                        ip_rotation_count += 1
                        rotated, _ = await asyncio.to_thread(rotate_egress, "empty upstream stream")
                        if not rotated:
                            return stream_rate_limit_response(current_model)
                    else:
                        delay = compute_backoff_delay(attempt, INITIAL_BACKOFF)
                        await asyncio.sleep(delay)
                    continue
            else:
                with FlowContext():
                    try:
                        res_json = await asyncio.to_thread(
                            collect_chat_completion, response, current_model
                        )
                        usage = res_json.get("usage", {})
                        track_token_usage(
                            current_model,
                            prompt_tokens=usage.get("prompt_tokens", DEFAULT_PROMPT_TOKENS),
                            completion_tokens=usage.get("completion_tokens", DEFAULT_COMPLETION_TOKENS)
                        )
                        return JSONResponse(content=res_json)
                    except UpstreamAPIError as exc:
                        return upstream_error_response(exc)
                    except Exception:
                        track_token_usage(current_model, prompt_tokens=DEFAULT_PROMPT_TOKENS, completion_tokens=DEFAULT_COMPLETION_TOKENS)
                        return JSONResponse(
                            status_code=502,
                            content={"error": {"message": f"Upstream returned an unreadable stream for '{current_model}'.", "type": "upstream_error", "code": 502}}
                        )

        except Exception as e:
            log.error(f"[Attempt {attempt}/{MAX_RETRIES_ON_429}] Connection error for model '{current_model}': {type(e).__name__}: {e}")
            if attempt < MAX_RETRIES_ON_429:
                await asyncio.sleep(min(2 ** attempt, BACKOFF_CAP))
            continue

    if client_stream:
        return stream_rate_limit_response(current_model)
    log.error(f"All {MAX_RETRIES_ON_429} attempts exhausted for model '{current_model}'. Returning 503.")
    return JSONResponse(
        status_code=503,
        content={"error": {"message": f"Upstream unavailable after {MAX_RETRIES_ON_429} attempts. Please retry.", "type": "upstream_error", "code": 503}},
        headers={"Retry-After": "10"}
    )

# -----------------------------------------------------------------------------
# Anthropic API Compatibility Endpoint (/v1/messages)
# -----------------------------------------------------------------------------
@app.post("/v1/messages")
async def anthropic_messages(raw_request: Request):
    metrics["total_requests"] += 1
    prom_requests_total.labels(model="messages", endpoint="anthropic_messages").inc()
    await wait_for_rotation_drain()

    start_time = time.time()
    try:
        body = await raw_request.json()
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise HTTPException(status_code=400, detail="Request body must contain valid JSON") from exc

    model_name = body.get("model") or get_default_model()
    client_stream = bool(body.get("stream", False))
    body = apply_free_request_shape(body, "messages")
    log.info(f"Received Anthropic-format request for model '{model_name}' (Stream: {client_stream})")

    headers = build_upstream_headers(raw_request, "messages")

    ip_rotation_count = 0
    for attempt in range(1, MAX_RETRIES_ON_429 + 1):
        try:
            await asyncio.sleep(random.uniform(0.1, 0.3))
            proxies = get_next_outbound_proxy()
            session = create_fresh_session(True) if client_stream else _get_session("anthropic")
            response = session.post(
                TARGET_ZEN_ANTHROPIC_URL,
                json=body,
                headers=headers,
                impersonate="chrome124",
                stream=True,
                proxies=proxies,
                timeout=STREAM_TIMEOUT,
            )
            log_upstream_response(response, model_name, "messages", attempt, proxies is not None)

            if response.status_code == 429:
                metrics["rate_limited_requests"] += 1
                prom_requests_rate_limited.labels(model=model_name).inc()
                if attempt < MIN_ATTEMPTS_BEFORE_RATE_LIMIT:
                    delay = compute_backoff_delay(attempt, INITIAL_BACKOFF)
                    log.warning(
                        "Upstream 429 for '%s'; retrying before returning rate_limit (%s/%s) in %.2fs.",
                        model_name,
                        attempt,
                        MIN_ATTEMPTS_BEFORE_RATE_LIMIT,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                if ip_rotation_count < MAX_IP_ROTATIONS_BEFORE_RATE_LIMIT:
                    ip_rotation_count += 1
                    rotated, verified_ip = await asyncio.to_thread(rotate_egress, "upstream rate limit")
                    if rotated:
                        log.warning("Retrying '%s' after IP rotation to %s.", model_name, verified_ip or "unknown")
                        continue
                    log.error("IP rotation failed for '%s' after upstream rate limits.", model_name)
                return rate_limit_response(response, model_name)

            if response.status_code >= 500:
                delay = compute_backoff_delay(attempt, INITIAL_BACKOFF)
                log.warning("Upstream HTTP %s for '%s'; retrying without egress rotation in %.2fs.", response.status_code, model_name, delay)
                await asyncio.sleep(delay)
                continue

            if response.status_code >= 400:
                return upstream_status_response(response, model_name)

            metrics["successful_requests"] += 1
            prom_requests_success.labels(model=model_name).inc()
            prom_request_duration.labels(model=model_name, endpoint="anthropic_messages").observe(time.time() - start_time)

            if client_stream:
                try:
                    stream_gen = await prepare_stream_response(
                        response, model_name, session=session, done_marker=b""
                    )
                except UpstreamAPIError as exc:
                    return upstream_error_response(exc)
                except EmptyStreamError:
                    if attempt >= MIN_ATTEMPTS_BEFORE_RATE_LIMIT and ip_rotation_count < MAX_IP_ROTATIONS_BEFORE_RATE_LIMIT:
                        ip_rotation_count += 1
                        rotated, _ = await asyncio.to_thread(rotate_egress, "empty upstream stream")
                        if not rotated:
                            return stream_rate_limit_response(model_name)
                    else:
                        await asyncio.sleep(compute_backoff_delay(attempt, INITIAL_BACKOFF))
                    continue
                return StreamingResponse(
                    stream_gen,
                    media_type="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
                )
            else:
                with FlowContext():
                    try:
                        res_json = await asyncio.to_thread(
                            collect_anthropic_message, response, model_name
                        )
                        usage = res_json.get("usage", {})
                        track_token_usage(
                            model_name,
                            prompt_tokens=usage.get("input_tokens", DEFAULT_PROMPT_TOKENS),
                            completion_tokens=usage.get("output_tokens", DEFAULT_COMPLETION_TOKENS),
                        )
                        return JSONResponse(content=res_json)
                    except UpstreamAPIError as exc:
                        return upstream_error_response(exc)
                    except Exception:
                        track_token_usage(model_name, prompt_tokens=DEFAULT_PROMPT_TOKENS, completion_tokens=DEFAULT_COMPLETION_TOKENS)
                        return JSONResponse(
                            status_code=502,
                            content={"error": {"message": f"Upstream returned an unreadable stream for '{model_name}'.", "type": "upstream_error", "code": 502}}
                        )

        except Exception as e:
            log.error(f"[Attempt {attempt}/{MAX_RETRIES_ON_429}] Anthropic endpoint error for model '{model_name}': {type(e).__name__}: {e}")
            if attempt < MAX_RETRIES_ON_429:
                await asyncio.sleep(min(2 ** attempt, BACKOFF_CAP))
            continue

    if client_stream:
        return stream_rate_limit_response(model_name)
    log.error(f"All {MAX_RETRIES_ON_429} attempts exhausted for Anthropic model '{model_name}'. Returning 503.")
    return JSONResponse(
        status_code=503,
        content={"error": {"message": f"Upstream unavailable after {MAX_RETRIES_ON_429} attempts. Please retry.", "type": "upstream_error", "code": 503}},
        headers={"Retry-After": "10"}
    )

@app.post("/v1/responses")
async def responses_endpoint(raw_request: Request):
    metrics["total_requests"] += 1
    prom_requests_total.labels(model="responses", endpoint="responses").inc()
    await wait_for_rotation_drain()

    start_time = time.time()
    try:
        body = await raw_request.json()
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise HTTPException(status_code=400, detail="Request body must contain valid JSON") from exc

    model_name = body.get("model") or get_default_model()
    client_stream = bool(body.get("stream", False))
    body = apply_free_request_shape(body, "responses")
    log.info(f"Received Responses API request for model '{model_name}' (Stream: {client_stream})")

    headers = build_upstream_headers(raw_request, "responses")

    ip_rotation_count = 0
    for attempt in range(1, MAX_RETRIES_ON_429 + 1):
        try:
            await asyncio.sleep(random.uniform(0.1, 0.3))
            proxies = get_next_outbound_proxy()
            session = create_fresh_session(True) if client_stream else _get_session("responses")
            response = session.post(
                TARGET_ZEN_RESPONSES_URL,
                json=body,
                headers=headers,
                impersonate="chrome124",
                stream=True,
                proxies=proxies,
                timeout=STREAM_TIMEOUT,
            )
            log_upstream_response(response, model_name, "responses", attempt, proxies is not None)

            if response.status_code == 429:
                metrics["rate_limited_requests"] += 1
                prom_requests_rate_limited.labels(model=model_name).inc()
                if attempt < MIN_ATTEMPTS_BEFORE_RATE_LIMIT:
                    delay = compute_backoff_delay(attempt, INITIAL_BACKOFF)
                    log.warning(
                        "Upstream 429 for '%s'; retrying before returning rate_limit (%s/%s) in %.2fs.",
                        model_name,
                        attempt,
                        MIN_ATTEMPTS_BEFORE_RATE_LIMIT,
                        delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                if ip_rotation_count < MAX_IP_ROTATIONS_BEFORE_RATE_LIMIT:
                    ip_rotation_count += 1
                    rotated, verified_ip = await asyncio.to_thread(rotate_egress, "upstream rate limit")
                    if rotated:
                        log.warning("Retrying '%s' after IP rotation to %s.", model_name, verified_ip or "unknown")
                        continue
                    log.error("IP rotation failed for '%s' after upstream rate limits.", model_name)
                return rate_limit_response(response, model_name)

            if response.status_code >= 500:
                delay = compute_backoff_delay(attempt, INITIAL_BACKOFF)
                log.warning("Upstream HTTP %s for '%s'; retrying without egress rotation in %.2fs.", response.status_code, model_name, delay)
                await asyncio.sleep(delay)
                continue

            if response.status_code >= 400:
                return upstream_status_response(response, model_name)

            metrics["successful_requests"] += 1
            prom_requests_success.labels(model=model_name).inc()
            prom_request_duration.labels(model=model_name, endpoint="responses").observe(time.time() - start_time)

            if client_stream:
                try:
                    stream_gen = await prepare_stream_response(
                        response, model_name, session=session, done_marker=b""
                    )
                except UpstreamAPIError as exc:
                    return upstream_error_response(exc)
                except EmptyStreamError:
                    if attempt >= MIN_ATTEMPTS_BEFORE_RATE_LIMIT and ip_rotation_count < MAX_IP_ROTATIONS_BEFORE_RATE_LIMIT:
                        ip_rotation_count += 1
                        rotated, _ = await asyncio.to_thread(rotate_egress, "empty upstream stream")
                        if not rotated:
                            return stream_rate_limit_response(model_name)
                    else:
                        await asyncio.sleep(compute_backoff_delay(attempt, INITIAL_BACKOFF))
                    continue
                return StreamingResponse(
                    stream_gen,
                    media_type="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
                )
            else:
                with FlowContext():
                    try:
                        res_json = await asyncio.to_thread(
                            collect_responses_api, response, model_name
                        )
                        return JSONResponse(content=res_json)
                    except UpstreamAPIError as exc:
                        return upstream_error_response(exc)
                    except Exception:
                        return JSONResponse(
                            status_code=502,
                            content={"error": {"message": f"Upstream returned an unreadable stream for '{model_name}'.", "type": "upstream_error", "code": 502}}
                        )

        except Exception as e:
            log.error(f"[Attempt {attempt}/{MAX_RETRIES_ON_429}] Responses endpoint error for model '{model_name}': {type(e).__name__}: {e}")
            if attempt < MAX_RETRIES_ON_429:
                await asyncio.sleep(min(2 ** attempt, BACKOFF_CAP))
            continue

    if client_stream:
        return stream_rate_limit_response(model_name)
    log.error(f"All {MAX_RETRIES_ON_429} attempts exhausted for Responses model '{model_name}'. Returning 503.")
    return JSONResponse(
        status_code=503,
        content={"error": {"message": f"Upstream unavailable after {MAX_RETRIES_ON_429} attempts. Please retry.", "type": "upstream_error", "code": 503}},
        headers={"Retry-After": "10"}
    )

# Global exception handler for standard error format
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    log.error(f"Unhandled error on {request.method} {request.url.path}: {exc}")
    return JSONResponse(
        status_code=500,
        content={"error": {"message": "Internal server error", "type": "internal_error", "code": 500}},
    )

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {"message": exc.detail, "type": "http_error", "code": exc.status_code}},
    )

if __name__ == "__main__":
    load_proxy_list()
    log.info(f"Starting OpenCode IP Proxy Server on {HOST}:{PORT}...")
    uvicorn.run(app, host=HOST, port=PORT)

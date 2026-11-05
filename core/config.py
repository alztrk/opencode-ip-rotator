import os
from pathlib import Path

# Upstream target URLs
OPENCODE_ZEN_TARGET_BASE = os.environ.get(
    "OPENCODE_ZEN_TARGET_BASE", "https://opencode.ai/inference/openai/v1"
)
CHAT_COMPLETIONS_URL = f"{OPENCODE_ZEN_TARGET_BASE}/chat/completions"
RESPONSES_URL = f"{OPENCODE_ZEN_TARGET_BASE}/responses"
ANTHROPIC_MESSAGES_URL = os.environ.get(
    "OPENCODE_ANTHROPIC_URL", "https://opencode.ai/inference/anthropic/v1/messages"
)
MODELS_URL = os.environ.get(
    "OPENCODE_MODELS_URL", "https://opencode.ai/inference/v1/models"
)

# OpenSSL / Hyper compliant ciphers for OpenCode compatibility
TLS12_CIPHERS = (
    "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:"
    "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:"
    "ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:"
    "ECDHE-ECDSA-AES128-SHA:ECDHE-RSA-AES128-SHA:"
    "ECDHE-ECDSA-AES256-SHA:ECDHE-RSA-AES256-SHA:"
    "AES128-GCM-SHA256:AES256-GCM-SHA384:AES128-SHA:AES256-SHA"
)

# Operational settings
DEFAULT_DATA_DIR = (
    Path("/app/data")
    if Path("/.dockerenv").exists()
    else Path(__file__).resolve().parent.parent / "data"
)
DEFAULT_MODEL = os.environ.get("DEFAULT_MODEL", "big-pickle")
DEFAULT_MODEL_OVERRIDE = os.environ.get("OPENCODE_DEFAULT_MODEL", "").strip()
MODEL_DISCOVERY_INTERVAL = int(os.environ.get("MODEL_DISCOVERY_INTERVAL", "300"))
STREAM_TIMEOUT = int(os.environ.get("STREAM_TIMEOUT", "120"))
WARP_ROTATOR_URL = os.environ.get("WARP_ROTATOR_URL", "http://warp-rotator:8001")
METRICS_DB_PATH = Path(
    os.environ.get("METRICS_DB_PATH", str(DEFAULT_DATA_DIR / "metrics.db"))
)
PROXY_LIST_FILE = Path(
    os.environ.get("PROXY_LIST_FILE", str(DEFAULT_DATA_DIR / "proxies.txt"))
)
CORS_ALLOW_ORIGINS = [
    origin.strip()
    for origin in os.environ.get("CORS_ALLOW_ORIGINS", "*").split(",")
    if origin.strip()
]

# Safe headers for upstream logging
SAFE_UPSTREAM_HEADERS = {
    "content-type",
    "retry-after",
    "x-request-id",
    "x-ratelimit-limit",
    "x-ratelimit-remaining",
    "x-ratelimit-reset",
    "cf-ray",
}
SENSITIVE_LOG_KEYS = {
    "authorization",
    "api_key",
    "apikey",
    "token",
    "password",
    "secret",
}

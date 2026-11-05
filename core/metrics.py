import time
from typing import Any, Dict

from prometheus_client import Counter, Gauge, Histogram

# In-memory metrics dict for dashboard & quick introspection
STARTUP_TIME = time.time()

metrics: Dict[str, Any] = {
    "total_requests": 0,
    "successful_requests": 0,
    "rate_limited_requests": 0,
    "failed_requests": 0,
    "discovered_models_count": 0,
}

# Prometheus metrics
prom_requests_total = Counter(
    "proxy_requests_total", "Total proxied requests", ["model", "endpoint"]
)
prom_requests_success = Counter(
    "proxy_requests_success", "Successful proxied requests", ["model"]
)
prom_requests_rate_limited = Counter(
    "proxy_requests_rate_limited", "Rate-limited requests", ["model"]
)
prom_request_duration = Histogram(
    "proxy_request_duration_seconds",
    "Request duration in seconds",
    ["model", "endpoint"],
    buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0),
)
prom_active_flows = Gauge("proxy_active_flows", "Active in-flight streaming requests")
prom_warp_health = Gauge("proxy_warp_health", "WARP rotator health status (1=healthy, 0=unhealthy)")

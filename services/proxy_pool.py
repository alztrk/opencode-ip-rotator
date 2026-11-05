import logging
import os
import threading
from typing import Dict, List, Optional

from core.config import PROXY_LIST_FILE

log = logging.getLogger("services.proxy_pool")
_proxy_pool: List[str] = []
_proxy_index = 0
_proxy_lock = threading.Lock()


def load_proxy_list() -> None:
    global _proxy_pool
    proxies = []
    if PROXY_LIST_FILE.exists():
        try:
            with open(PROXY_LIST_FILE, "r", encoding="utf-8") as f:
                lines = [
                    line.strip()
                    for line in f
                    if line.strip() and not line.startswith("#")
                ]
                proxies.extend(lines)
        except Exception as e:
            log.error("Error reading proxy file %s: %e", PROXY_LIST_FILE, e)

    env_proxies = os.environ.get("PROXY_LIST", "").strip()
    if env_proxies:
        proxies.extend([p.strip() for p in env_proxies.split(",") if p.strip()])

    with _proxy_lock:
        _proxy_pool = list(dict.fromkeys(proxies))
        if _proxy_pool:
            log.info("Loaded %d proxies into outbound pool.", len(_proxy_pool))


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

    return None

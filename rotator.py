import ctypes
import json
import logging
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
CHECK_ENDPOINT = os.environ.get("WARP_CHECK_ENDPOINT", "https://opencode.ai")
CHECK_INTERVAL = int(os.environ.get("WARP_CHECK_INTERVAL", "15"))
PERIODIC_ROTATION_INTERVAL = int(os.environ.get("WARP_ROTATION_INTERVAL", "300"))

INITIAL_RETRY_DELAY = int(os.environ.get("WARP_RETRY_DELAY", "3"))
MAX_RETRIES = int(os.environ.get("WARP_MAX_RETRIES", "5"))
AUTO_RECYCLE_THRESHOLD = int(os.environ.get("AUTO_RECYCLE_THRESHOLD", "50"))
CUSTOM_OUTBOUND_PROXY = os.environ.get("CUSTOM_OUTBOUND_PROXY", "").strip()

# Proxy Pool Configuration
DEFAULT_DATA_DIR = Path("/app/data") if Path("/.dockerenv").exists() else Path(__file__).resolve().parent / "data"
PROXY_LIST_FILE = os.environ.get("PROXY_LIST_FILE", str(DEFAULT_DATA_DIR / "proxies.txt"))
PROXY_LIST_ENV = os.environ.get("PROXY_LIST", "").strip()
_proxy_pool: List[str] = []
_proxy_index = 0
_proxy_lock = threading.Lock()
_last_proxy_mtime: float = 0.0

def load_proxy_list(force: bool = False) -> None:
    """Load proxy list from file and environment variable with hot-reloading."""
    global _proxy_pool, _proxy_index, _last_proxy_mtime
    with _proxy_lock:
        lines = []
        files_to_check = [Path(PROXY_LIST_FILE)]
        root_file = Path(__file__).resolve().parent / "proxies.txt"
        if root_file != files_to_check[0] and root_file.exists():
            files_to_check.append(root_file)

        for proxy_file in files_to_check:
            if proxy_file.exists():
                try:
                    mtime = proxy_file.stat().st_mtime
                    if not force and mtime == _last_proxy_mtime and _proxy_pool:
                        return
                    _last_proxy_mtime = mtime
                    with open(proxy_file, "r", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if line and not line.startswith("#"):
                                if "://" not in line:
                                    line = f"http://{line}"
                                lines.append(line)
                except Exception as e:
                    log.error(f"Error reading proxy list file {proxy_file}: {e}")

        if PROXY_LIST_ENV:
            for p in PROXY_LIST_ENV.split(","):
                p = p.strip()
                if p:
                    if "://" not in p:
                        p = f"http://{p}"
                    lines.append(p)

        _proxy_pool = list(dict.fromkeys(lines))
        _proxy_index = 0
        if _proxy_pool:
            log.info(f"Loaded {len(_proxy_pool)} proxies into rotation pool.")
        if False:
            lines = []
            for p in PROXY_LIST_ENV.split(","):
                p = p.strip()
                if p:
                    if "://" not in p:
                        p = f"http://{p}"
                    lines.append(p)
            _proxy_pool = list(dict.fromkeys(lines))
            _proxy_index = 0
            if _proxy_pool:
                log.info(f"Loaded {len(_proxy_pool)} proxies into rotation pool.")
        else:
            _proxy_pool = []
            _proxy_index = 0

def get_next_proxy() -> Optional[Dict[str, str]]:
    """Get the next proxy from the pool in round-robin fashion."""
    global _proxy_index
    load_proxy_list()
    with _proxy_lock:
        if not _proxy_pool:
            return None
        proxy_url = _proxy_pool[_proxy_index % len(_proxy_pool)]
        _proxy_index += 1
        return {"http": proxy_url, "https": proxy_url}

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

LOG_FORMAT = os.environ.get("LOG_FORMAT", "text").lower()
if LOG_FORMAT == "json":
    _handler = logging.StreamHandler()
    _handler.setFormatter(JSONFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[_handler])
else:
    logging.basicConfig(
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
log = logging.getLogger("rotator")

rotation_lock = threading.Lock()
active_flows_count = 0
flow_lock = threading.Lock()
_current_ip: Optional[str] = None
rotation_count = 0
FLOW_LEASE_DB_PATH = Path(os.environ.get("METRICS_DB_PATH", str(DEFAULT_DATA_DIR / "metrics.db")))

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def has_active_flow_leases() -> bool:
    """Read proxy-owned stream leases from the shared metrics database."""
    if not FLOW_LEASE_DB_PATH.exists():
        return False
    try:
        conn = sqlite3.connect(str(FLOW_LEASE_DB_PATH), timeout=5)
        try:
            conn.execute("DELETE FROM active_flow_leases WHERE expires_at <= ?", (time.time(),))
            row = conn.execute("SELECT 1 FROM active_flow_leases LIMIT 1").fetchone()
            return row is not None
        finally:
            conn.close()
    except sqlite3.OperationalError as exc:
        # During proxy startup the table may not exist yet; never turn a DB
        # read race into an unguarded rotation.
        if "no such table" not in str(exc).lower():
            log.warning("Unable to inspect active stream leases: %s", exc)
        return True


def get_public_ip() -> Optional[str]:
    """Fetches current public IP using Chrome TLS impersonation."""
    try:
        from curl_cffi import requests
        resp = requests.get("https://api.ipify.org?format=json", impersonate="chrome124", timeout=5)
        if resp.status_code == 200:
            return resp.json().get("ip")
    except Exception:
        try:
            from curl_cffi import requests
            resp = requests.get("https://ifconfig.me/ip", impersonate="chrome124", timeout=5)
            if resp.status_code == 200:
                return resp.text.strip()
        except Exception:
            return None


def get_public_ip_via_proxy(proxy: Dict[str, str]) -> Optional[str]:
    """Fetches current public IP using a specific proxy."""
    try:
        from curl_cffi import requests
        resp = requests.get("https://api.ipify.org?format=json", impersonate="chrome124", timeout=10, proxies=proxy)
        if resp.status_code == 200:
            return resp.json().get("ip")
    except Exception:
        try:
            from curl_cffi import requests
            resp = requests.get("https://ifconfig.me/ip", impersonate="chrome124", timeout=10, proxies=proxy)
            if resp.status_code == 200:
                return resp.text.strip()
        except Exception:
            return None
    return None


ip_history: List[Dict[str, Any]] = []

def get_ip_location(ip: str) -> Dict[str, str]:
    """Fetches country, flag emoji, and location details for a given IP."""
    if not ip:
        return {"country": "Unknown", "countryCode": "UN", "flag": "🌐"}
    try:
        from curl_cffi import requests
        resp = requests.get(f"http://ip-api.com/json/{ip}", impersonate="chrome124", timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            country_code = data.get("countryCode", "UN")
            # Generate flag emoji from country code
            flag = "".join(chr(127397 + ord(c)) for c in country_code) if len(country_code) == 2 else "🌐"
            return {
                "country": data.get("country", "Unknown"),
                "countryCode": country_code,
                "city": data.get("city", ""),
                "flag": flag
            }
    except Exception:
        pass
    return {"country": "Unknown", "countryCode": "UN", "flag": "🌐"}

def is_admin() -> bool:
    if os.name != "nt":
        return True
    try:
        return ctypes.windll.shell32.IsUserAnAdmin() != 0
    except Exception:
        return False

def elevate() -> None:
    if os.name == "nt" and not is_admin():
        log.info("Requesting administrative privileges...")
        params = subprocess.list2cmdline(sys.argv)
        try:
            ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, params, None, 1)
        except Exception as e:
            log.error(f"Failed to elevate: {e}")
        sys.exit()

def get_warp_bin() -> str:
    path = shutil.which("warp-cli")
    if path:
        return path
    candidates = [
        r"C:\Program Files\Cloudflare\Cloudflare WARP\warp-cli.exe",
        r"C:\Program Files (x86)\Cloudflare\Cloudflare WARP\warp-cli.exe",
        "/usr/bin/warp-cli",
        "/usr/local/bin/warp-cli",
        "/bin/warp-cli",
    ]
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    return "warp-cli"

# -----------------------------------------------------------------------------
# WARP Controller with IP Verification & Auto-Recycle Trigger
# -----------------------------------------------------------------------------
def rotate_warp(reason: str = "Triggered") -> bool:
    global _current_ip, rotation_count
    with rotation_lock:
        with flow_lock:
            if active_flows_count > 0 or has_active_flow_leases():
                log.info("IP rotation skipped — an active streaming flow lease is in progress.")
                return False

            old_ip = _current_ip or get_public_ip()
            log.info(f"Initiating guaranteed IP rotation... (Reason: {reason} | Current IP: {old_ip})")

            # Try local WARP CLI rotation first
            warp_bin = get_warp_bin()
            if shutil.which(warp_bin) or os.path.exists(warp_bin):
                max_attempts = 4
                for attempt in range(1, max_attempts + 1):
                    try:
                        log.info(f"WARP rotation attempt {attempt}/{max_attempts}...")
                        subprocess.run([warp_bin, "--accept-tos", "disconnect"], capture_output=True, text=True, timeout=10, check=False)
                        time.sleep(1)

                        subprocess.run([warp_bin, "--accept-tos", "registration", "delete"], capture_output=True, text=True, timeout=10, check=False)
                        for _ in range(10):
                            time.sleep(0.3)
                            chk = subprocess.run([warp_bin, "--accept-tos", "registration", "show"], capture_output=True, text=True, check=False)
                            if "Missing registration" in chk.stderr or "Missing registration" in chk.stdout:
                                break
                        subprocess.run([warp_bin, "--accept-tos", "registration", "new"], capture_output=True, text=True, timeout=10, check=False)
                        time.sleep(0.5)
                        if not Path("/.dockerenv").exists():
                            subprocess.run([warp_bin, "--accept-tos", "mode", "proxy"], capture_output=True, text=True, timeout=10, check=False)
                            time.sleep(0.3)

                        res = subprocess.run([warp_bin, "--accept-tos", "connect"], capture_output=True, text=True, timeout=10, check=False)

                        if res.returncode == 0:
                            time.sleep(3)
                            new_ip = get_public_ip()

                            if new_ip and new_ip != old_ip:
                                _current_ip = new_ip
                                rotation_count += 1
                                loc = get_ip_location(new_ip)

                                timestamp_str = time.strftime("%H:%M:%S", time.localtime())
                                ip_history.append({
                                    "ip": new_ip,
                                    "country": loc.get("country", "Unknown"),
                                    "flag": loc.get("flag", "🌐"),
                                    "timestamp": timestamp_str,
                                    "reason": reason
                                })
                                if len(ip_history) > 20:
                                    ip_history.pop(0)

                                try:
                                    db_path = Path(os.environ.get("METRICS_DB_PATH", "/app/data/metrics.db"))
                                    if db_path.exists():
                                        conn = sqlite3.connect(str(db_path))
                                        cursor = conn.cursor()
                                        cursor.execute(
                                            "INSERT INTO ip_history (ip, country, flag, timestamp, reason) VALUES (?, ?, ?, ?, ?)",
                                            (new_ip, loc.get("country", "Unknown"), loc.get("flag", "🌐"), timestamp_str, reason)
                                        )
                                        conn.commit()
                                        conn.close()
                                except Exception as err:
                                    log.error(f"Failed to write IP rotation to SQLite DB: {err}")

                                log.info(f"Guaranteed WARP IP rotation successful! New Verified IP: {new_ip} {loc.get('flag')} ({loc.get('country')}) (Total Rotations: {rotation_count})")

                                if rotation_count >= AUTO_RECYCLE_THRESHOLD:
                                    log.warning(f"Auto-recycle threshold reached ({rotation_count}/{AUTO_RECYCLE_THRESHOLD}). Triggering container refresh...")
                                    trigger_container_recycle()

                                return True
                            else:
                                log.warning(f"Attempt {attempt}: Assigned IP ({new_ip}) was identical to old IP ({old_ip}). Retrying fresh registration...")
                    except FileNotFoundError:
                        log.error(f"Cloudflare WARP CLI ('{warp_bin}') was not found. Please install Cloudflare WARP and add warp-cli to PATH.")
                        break
                    except Exception as e:
                        log.error(f"Error during WARP rotation attempt {attempt}: {e}")
                        time.sleep(1)
            else:
                log.warning("WARP CLI not available locally. Trying remote rotator service...")

            # Try remote rotator service as fallback
            rotator_endpoints = ["http://warp-rotator:8001/rotate", "http://127.0.0.1:8001/rotate"]
            for endpoint in rotator_endpoints:
                try:
                    req = Request(endpoint, data=b"", headers={"User-Agent": "rotator-fallback"}, method="POST")
                    with urlopen(req, timeout=35) as resp:
                        if resp.status == 200:
                            res_data = json.loads(resp.read().decode("utf-8"))
                            if res_data.get("status") == "success":
                                _current_ip = res_data.get("verified_ip", _current_ip)
                                log.info(f"Rotation via remote rotator service ({endpoint}) successful. Verified IP: {_current_ip}")
                                return True
                except Exception:
                    pass

            # Try proxy rotation as final fallback
            log.warning("WARP and remote rotator unavailable. Attempting proxy rotation...")
            proxy = get_next_proxy()
            if proxy:
                new_ip = get_public_ip_via_proxy(proxy)
                if new_ip and new_ip != old_ip:
                    _current_ip = new_ip
                    rotation_count += 1
                    loc = get_ip_location(new_ip)

                    timestamp_str = time.strftime("%H:%M:%S", time.localtime())
                    ip_history.append({
                        "ip": new_ip,
                        "country": loc.get("country", "Unknown"),
                        "flag": loc.get("flag", "🌐"),
                        "timestamp": timestamp_str,
                        "reason": f"{reason} (via proxy)"
                    })
                    if len(ip_history) > 20:
                        ip_history.pop(0)

                    log.info(f"Proxy IP rotation successful! New Verified IP: {new_ip} {loc.get('flag')} ({loc.get('country')}) (Total Rotations: {rotation_count})")
                    return True
                else:
                    log.warning("Proxy rotation failed to provide a different IP.")
            else:
                log.warning("No proxies available for rotation.")

            log.error("All IP rotation methods failed (WARP, remote rotator, proxy).")
            return False

def trigger_container_recycle():
    """Triggers self-destruction/recycle script if inside container."""
    try:
        subprocess.Popen(["python3", "manager.py"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:
        log.error(f"Failed to trigger auto-recycle: {e}")

def handle_rate_limit(attempt: int, initial_delay: int, max_retries: int) -> bool:
    delay = initial_delay * (2 ** (attempt - 1))
    log.warning(f"HTTP 429 Rate Limit detected! Retry attempt {attempt}/{max_retries} — Backoff delay: {delay}s")
    time.sleep(delay)
    return rotate_warp(reason=f"HTTP 429 - Attempt {attempt}")

# -----------------------------------------------------------------------------
# Background Monitors
# -----------------------------------------------------------------------------
def health_check_loop(endpoint: str, interval: int, initial_delay: int, max_retries: int, stop_event: threading.Event) -> None:
    log.info(f"Health check monitor started. Endpoint: {endpoint} (Interval: {interval}s)")
    retry_count = 0

    while not stop_event.is_set():
        try:
            req = Request(endpoint, headers={"User-Agent": "WARP-Guard/1.0"}, method="HEAD")
            with urlopen(req, timeout=10) as resp:
                if resp.status == 200:
                    retry_count = 0
        except HTTPError as e:
            if e.code == 429:
                retry_count += 1
                if retry_count <= max_retries:
                    handle_rate_limit(retry_count, initial_delay, max_retries)
                else:
                    log.error(f"Maximum retry attempts ({max_retries}) reached. Pausing health check for 30s.")
                    time.sleep(30)
                    retry_count = 0
        except Exception as e:
            log.debug(f"Health check error: {e}")

        stop_event.wait(interval)

def periodic_rotation_loop(interval: int, stop_event: threading.Event) -> None:
    if interval <= 0:
        return

    log.info(f"Periodic IP rotator started. (Interval: {interval}s)")
    while not stop_event.is_set():
        if stop_event.wait(interval):
            break
        if has_active_flow_leases():
            log.info("Scheduled IP rotation deferred — an active streaming flow lease is in progress.")
            continue
        rotate_warp(reason="Scheduled Interval")

def start_rotator_http_server():
    """Starts a lightweight HTTP server inside warp-rotator container on port 8001 to handle remote rotate requests."""
    try:
        from fastapi import FastAPI
        import uvicorn
        
        rotator_app = FastAPI()
        
        @rotator_app.get("/health")
        def http_health():
            return {"status": "healthy", "current_ip": _current_ip, "rotations": rotation_count}

        @rotator_app.post("/rotate")
        def http_rotate():
            success = rotate_warp(reason="Remote HTTP Dashboard Trigger")
            return {"status": "success" if success else "failed", "verified_ip": _current_ip}
        
        @rotator_app.get("/status")
        def http_status():
            return {"current_ip": _current_ip, "rotations": rotation_count, "history": ip_history}

        uvicorn.run(rotator_app, host="0.0.0.0", port=8001, log_level="warning")
    except Exception as e:
        log.error(f"Failed to start rotator HTTP listener: {e}")

def _cleanup_warp():
    warp_bin = get_warp_bin()
    if not shutil.which(warp_bin) and not os.path.exists(warp_bin):
        return
    log.info("Disconnecting WARP and cleaning up...")
    try:
        subprocess.run([warp_bin, "--accept-tos", "disconnect"], capture_output=True, text=True, timeout=10, check=False)
        subprocess.run([warp_bin, "--accept-tos", "registration", "delete"], capture_output=True, text=True, timeout=10, check=False)
    except Exception as e:
        log.warning(f"Error during WARP cleanup: {e}")
    log.info("WARP cleanup complete.")

def main() -> None:
    elevate()
    load_proxy_list()
    global _current_ip
    _current_ip = get_public_ip()
    log.info(f"Starting IP Rotator Node... Initial Verified Public IP: {_current_ip}")

    stop_event = threading.Event()

    def _handle_signal(signum, frame):
        log.warning(f"Received signal {signum}, shutting down rotator...")
        stop_event.set()
        _cleanup_warp()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    health_thread = threading.Thread(target=health_check_loop, args=(CHECK_ENDPOINT, CHECK_INTERVAL, INITIAL_RETRY_DELAY, MAX_RETRIES, stop_event), daemon=True)
    periodic_thread = threading.Thread(target=periodic_rotation_loop, args=(PERIODIC_ROTATION_INTERVAL, stop_event), daemon=True)
    http_thread = threading.Thread(target=start_rotator_http_server, daemon=True)

    health_thread.start()
    periodic_thread.start()
    http_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down Rotator...")
        stop_event.set()
        _cleanup_warp()

if __name__ == "__main__":
    main()

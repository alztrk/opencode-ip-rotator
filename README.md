# OpenCode IP Rotator & Proxy Server

[![GitHub release](https://img.shields.io/github/v/release/alztrk/opencode-ip-rotator?style=flat-square&color=blue)](https://github.com/alztrk/opencode-ip-rotator)
[![Docker Image](https://img.shields.io/badge/docker-microservices-blue.svg?style=flat-square&logo=docker)](https://github.com/alztrk/opencode-ip-rotator)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](https://opensource.org/licenses/MIT)
[![Python 3.11](https://img.shields.io/badge/python-3.11-brightgreen.svg?style=flat-square&logo=python)](https://python.org)

A microservice-architected Cloudflare WARP IP rotator and proxy server for OpenCode Zen. Designed to prevent HTTP 429 rate limits, guarantee unique IP rotation per cycle, log usage metrics into SQLite, and display real-time statistics on a clean web dashboard.

![OpenCode IP Rotator Dashboard Preview](docs/dashboard_preview.jpg)

---

## Key Features

- **Verified Shared Egress**: The proxy and WARP service share one network namespace, so the observed egress path is the path used for upstream requests.
- **Microservices Architecture**: Decoupled `proxy-server` (FastAPI) and `warp-rotator` (Cloudflare WARP daemon) services built with Docker Compose.
- **Clean Management Dashboard**: Lightweight Web UI displaying active connections, current location, token statistics, and manual rotation controls.
- **SQLite Data Persistence**: Stores token consumption, model request counts, and historical IP rotation logs on disk.
- **USD Savings Calculator**: Estimates cost savings per model based on prompt and completion token rates.
- **Table Pagination**: Built-in 5-item pagination for model usage and IP rotation log tables.
- **Active Flow Locking**: Protects active SSE streams from being interrupted during IP rotation — `_active_flows_count` is held for the **full lifetime of the generator**, not just until `return`.
- **Rate-limit Preservation**: Returns upstream 429 details and `Retry-After` without attempting to bypass model, account, provider, or subscription limits.
- **Anthropic API Compatibility**: Native `/v1/messages` endpoint for Claude clients and the Vercel AI SDK `@ai-sdk/anthropic` provider.
- **Custom Proxy Pool Support**: Round-robin outbound proxy pool via `data/proxies.txt` or `PROXY_LIST` environment variable.

---

## Architecture Overview

```
[OpenCode Client] ──(HTTP/2)──> [Proxy Server (Port 8000)] ──(SQLite)──> [metrics.db]
                                           │
                                  (Inter-Service IPC)
                                           ▼
                                [WARP Rotator (Port 8001)] ──> [Cloudflare WARP Daemon]
                                           │
                                           ▼
                              [OpenCode Zen API Endpoint]
```

### Core Architecture Components

1. **Proxy Server (`server.py`):** An OpenAI-compatible API proxy server running on `http://127.0.0.1:8000/v1`. It processes requests, forwards headers dynamically, handles streaming SSE responses, and presents a Web Management Dashboard.
2. **Rotator Module (`rotator.py`):** Owns the WARP network namespace and exposes a private health/rotation control endpoint. Manual rotation is authenticated; upstream 429 responses are not used as a rotation trigger.
3. **Container Manager (`manager.py`):** Provides automated ephemeral container lifecycle management. Triggers container self-destruction and re-creation once a defined rotation threshold is reached to ensure fresh hardware identifiers (`machine-id`).

---

## Detailed Features

- **OpenAI Standard Compatibility:** Fully exposes `/v1/chat/completions` and `/v1/models` endpoints to integrate with standard clients.
- **Dynamic Header Forwarding:** Captures and forwards all incoming client metadata including `x-opencode-*` headers and injects `Authorization: Bearer public` credentials required by the upstream API.
- **Verified Public IP Rotation:** Validates public IP changes via external IP lookup services to guarantee a distinct IP allocation after every disconnection cycle.
- **Active Flow Locking:** Prevents IP rotations during active Server-Sent Events (SSE) streaming sessions to prevent connection truncation and stream drops.
- **Dynamic Model Auto-Discovery:** Periodically queries the upstream catalog and detects free models from their pricing metadata, so newly available free models are picked up without code changes or static lists (including free models without a `free` suffix, such as `big-pickle`).
- **Web Management Dashboard:** Includes a clean, dark-themed web interface accessible at `http://127.0.0.1:8000/dashboard` for monitoring public IP status, active connections, total rotations, and triggering manual rotations.

---

## Quick Automated Setup

Run the automated installer script to install Python dependencies, verify system requirements, and automatically configure `~/.config/opencode/opencode.jsonc`:

```bash
python setup.py
```

---

## Installation & Deployment

### Option 1: Docker Container Deployment (Recommended)

Running the project in Docker isolates the execution environment, preventing local network configuration changes and ensuring a new environment identity (`/etc/machine-id`) on every initialization.

#### Prerequisites
- Docker Engine 20.10+
- Docker Compose v2+

#### Build and Launch
```bash
docker compose up -d --build
```

#### Access Web Dashboard
Open your browser and navigate to:
`http://127.0.0.1:8000/dashboard`

#### Environment Re-creation
To manually trigger environment self-destruction and re-create a container with fresh hardware identifiers:
```bash
python manager.py
```

---

### Option 2: Local Native Execution

#### Prerequisites
- Python 3.8 or higher
- Cloudflare WARP CLI (`warp-cli`) installed and added to system PATH
- Administrative privileges (required for `warp-cli` operations on Windows)

#### Steps

1. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Start the rotator background service:
   ```bash
   python rotator.py
   ```

3. Launch the proxy server:
   ```bash
   python server.py
   ```

---

## Configuration

### OpenAI-Compatible Provider (Default)

To use the local proxy server within OpenCode, update your configuration file at `~/.config/opencode/opencode.jsonc`:

```jsonc
{
  "provider": {
    "opencode-zen-local": {
      "npm": "@ai-sdk/openai-compatible",
      "options": {
        "baseURL": "http://127.0.0.1:8000/v1",
        "apiKey": "any"
      },
      "name": "OpenCode Zen Local Proxy"
    }
  }
}
```

> **Note:** Models are automatically discovered from the proxy server's `/v1/models` endpoint. You do not need to hardcode model names manually.

---

### Anthropic API Provider

The proxy exposes a native Anthropic-compatible `/v1/messages` endpoint. To use it with OpenCode's Anthropic provider:

```jsonc
{
  "provider": {
    "my-anthropic-proxy": {
      "npm": "@ai-sdk/anthropic",
      "options": {
        "baseURL": "http://127.0.0.1:8000",
        "apiKey": "any"
      }
    }
  }
}
```

> Requests sent to `/v1/messages` are translated to OpenAI format internally and routed through the same WARP-protected upstream.

---

### Custom Outbound Proxy Pool

If you want to use your own HTTP/SOCKS5 proxies instead of (or in addition to) Cloudflare WARP:

**Option 1 — File:** Create `data/proxies.txt` with one proxy per line:
```
http://user:pass@proxy1.example.com:8080
socks5://proxy2.example.com:1080
```

**Option 2 — Environment variable:**
```bash
PROXY_LIST="http://proxy1:8080,socks5://proxy2:1080" docker compose up -d
```

The proxy pool rotates in round-robin order across all outbound requests.

---

## API Endpoints Reference

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/v1/chat/completions` | `POST` | OpenAI-compatible chat completion endpoint with automatic retry and IP rotation. |
| `/v1/messages` | `POST` | Anthropic-compatible endpoint (`/v1/messages`) for Claude clients and `@ai-sdk/anthropic`. |
| `/v1/models` | `GET` | Returns list of currently discovered active free models. |
| `/dashboard` | `GET` | Renders the HTML Web Management Dashboard. |
| `/metrics` | `GET` | Returns structured JSON metrics including verified IP, uptime, and request counters. |
| `/api/rotate` | `POST` | Triggers an immediate manual IP rotation cycle. |

---

## Technical Specifications & Environment Variables

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `OPENCODE_ZEN_PORT` | `8000` | Local port for the proxy server. |
| `OPENCODE_ZEN_HOST` | `127.0.0.1` | Host address for binding the server (`0.0.0.0` in Docker). |
| `OPENCODE_MODELS_URL` | `https://opencode.ai/inference/v1/models` | Model catalog used for free-model auto-discovery. |
| `OPENCODE_DEFAULT_MODEL` | first discovered free model | Model used when a client omits the `model` field. |
| `WARP_CHECK_INTERVAL` | `15` | Health check interval in seconds. |
| `WARP_ROTATION_INTERVAL` | `300` | Periodic IP rotation interval in seconds. |
| `AUTO_RECYCLE_THRESHOLD` | `50` | Maximum rotations before triggering container environment refresh. |
| `CORS_ALLOW_ORIGINS` | `http://127.0.0.1:8000,http://localhost:8000` | Comma-separated browser origins allowed to call the proxy. |
| `WARP_ROTATOR_URL` | `http://127.0.0.1:8001` | Internal rotator endpoint. Do not expose port 8001 publicly. |

### Rate-limit behavior

The proxy preserves upstream `429` responses, including `Retry-After`, and does not treat them as a signal to bypass account, model, provider, or subscription limits. The dashboard can trigger manual WARP rotation. In Docker, the proxy shares the rotator's network namespace so the egress path being checked is the path used for upstream requests.

---

## Responsibility Disclaimer

This project is intended for educational, research, and infrastructure resilience testing purposes. Users are responsible for ensuring their usage complies with applicable terms of service and acceptable use policies of third-party service providers. The maintainers assume no liability for account suspensions, service interruptions, or misuse.

---

## License

This software is released under the [MIT License](LICENSE).

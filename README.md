# OpenCode IP Rotator & Proxy Server

[![GitHub release](https://img.shields.io/github/v/release/alztrk/opencode-ip-rotator?style=flat-square&color=blue)](https://github.com/alztrk/opencode-ip-rotator)
[![Docker](https://img.shields.io/badge/docker-compose-blue.svg?style=flat-square&logo=docker)](https://github.com/alztrk/opencode-ip-rotator)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-brightgreen.svg?style=flat-square&logo=python)](https://python.org)

A modular, lightweight proxy server for OpenCode's free inference tier. It handles upstream protocol requirements, TLS cipher matching, model auto-discovery, SSE streaming, and outbound IP rotation.

![OpenCode IP Rotator Dashboard Preview](docs/dashboard_preview.jpg)

---

## Features

- **OpenAI & Anthropic Compatible:** Exposes `/v1/chat/completions`, `/v1/messages`, and `/v1/models` for OpenCode, Claude clients, Cursor, or standard SDKs.
- **Protocol & TLS Compatibility:** Injects required OpenCode session headers, environment markers, and matches OpenSSL TLS 1.2 ciphers expected by upstream Cloudflare gates.
- **Dynamic Model Discovery:** Periodically checks OpenCode's catalog and automatically exposes all free models (including `big-pickle`) without manual configuration.
- **IP Rotation & Proxy Pool:** Rotates egress IPs via Cloudflare WARP or through a custom proxy pool defined in `proxies.txt`.
- **Stream Safety:** Protects active SSE streaming sessions from IP rotation interruptions until generation completes.
- **Web Dashboard & Metrics:** Real-time web UI at `/dashboard` displaying token counts, verified public IP, active connections, and SQLite history.

---

## Project Structure

```
opencode-ip-rotator/
├── server.py              # FastAPI application entrypoint
├── rotator.py             # Cloudflare WARP IP rotation service
├── core/
│   ├── config.py          # Centralized configuration and timeouts
│   ├── db.py              # SQLite storage for IP history and token metrics
│   └── metrics.py         # In-memory and Prometheus metrics
├── routes/
│   ├── chat.py            # /v1/chat/completions, /v1/messages, /v1/responses
│   ├── models.py          # /v1/models catalog endpoint
│   └── system.py          # /health, /status, /metrics, /dashboard
├── services/
│   ├── discovery.py       # OpenCode free model auto-discovery
│   ├── proxy_pool.py      # Round-robin outbound proxy rotation
│   └── upstream.py        # OpenCode HTTP transport, TLS ciphers & stream handling
├── proxies.txt            # Optional list of custom outbound proxies
├── docker-compose.yml     # Multi-container Docker deployment
└── tests/                 # Automated unit and integration tests
```

---

## Quickstart

### Method 1: Docker (Recommended)

1. Clone and start the containers:
   ```bash
   docker compose up -d --build
   ```

2. Open the dashboard:
   Navigate to `http://localhost:8000/dashboard`

3. Stop the services:
   ```bash
   docker compose down
   ```

### Method 2: Local Native Run

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Start the proxy server:
   ```bash
   python server.py
   ```

3. (Optional) Run the Cloudflare WARP rotator if `warp-cli` is installed locally:
   ```bash
   python rotator.py
   ```

---

## Client Configuration

### OpenCode (`~/.config/opencode/opencode.jsonc`)

#### Modern Schema (`providers`)

Add the local proxy using the current OpenCode provider schema:

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "providers": {
    "opencode-ip-rotator": {
      "name": "OpenCode IP Rotator",
      "package": "@opencode/ai/providers/openai-compatible",
      "settings": {
        "baseURL": "http://127.0.0.1:8000/v1"
      },
      "models": {
        "big-pickle": {
          "name": "Big Pickle"
        }
      }
    }
  }
}
```

#### Legacy Schema (`provider`)

For older OpenCode installations using the deprecated `@ai-sdk` schema:

```jsonc
{
  "provider": {
    "opencode-ip-rotator": {
      "npm": "@ai-sdk/openai-compatible",
      "options": {
        "baseURL": "http://127.0.0.1:8000/v1",
        "apiKey": "any"
      },
      "name": "OpenCode IP Rotator"
    }
  }
}
```

Or configure it as an Anthropic provider:

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

---

## Outbound Proxy Pool

If you have residential or datacenter proxies, you can route upstream requests through them:

1. Add your proxies to `proxies.txt` (one per line):
   ```text
   http://username:password@proxy1.example.com:8080
   socks5://proxy2.example.com:1080
   ```

2. Or set the `PROXY_LIST` environment variable:
   ```bash
   PROXY_LIST="http://proxy1:8080,socks5://proxy2:1080" docker compose up -d
   ```

Requests rotate across the pool in round-robin order.

---

## Endpoints

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/v1/chat/completions` | `POST` | OpenAI-compatible chat completion (streaming and non-streaming) |
| `/v1/messages` | `POST` | Anthropic-compatible chat completion |
| `/v1/models` | `GET` | Currently active free models with their full OpenCode catalog metadata |
| `/dashboard` | `GET` | Web management dashboard |
| `/metrics` | `GET` | Public IP, token metrics, and operational stats (JSON) |
| `/health` | `GET` | Health check probe |

---

## Environment Variables

| Variable | Default | Description |
| :--- | :--- | :--- |
| `PORT` | `8000` | Port for the proxy server |
| `DEFAULT_MODEL` | `big-pickle` | Fallback model if omitted by the client |
| `MODEL_DISCOVERY_INTERVAL` | `300` | Interval in seconds to refresh free models |
| `STREAM_TIMEOUT` | `120` | Maximum timeout for upstream streaming requests |
| `PROXY_LIST_FILE` | `data/proxies.txt` | Path to custom proxy pool file |
| `CUSTOM_OUTBOUND_PROXY` | `""` | Single static outbound proxy URL |
| `WARP_ROTATOR_URL` | `http://warp-rotator:8001` | Internal URL for the WARP rotator service |

---

## Testing

Run unit and integration tests:

```bash
pytest
```

Run end-to-end smoke tests against a live instance:

```bash
python test.py
```

---

## License

This project is licensed under the [MIT License](LICENSE).

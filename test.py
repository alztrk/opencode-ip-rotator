import json
import os
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

PROXY_URL = os.environ.get("PROXY_URL", "http://127.0.0.1:8000")

def log(msg, status="INFO"):
    print(f"[{status}] {msg}")

def test_models_endpoint():
    log("Testing GET /v1/models (Dynamic Model Discovery)...")
    try:
        req = Request(f"{PROXY_URL}/v1/models", headers={"User-Agent": "OpenCode-Tester/1.0"})
        with urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                data = json.loads(resp.read().decode("utf-8"))
                models = [m.get("id") for m in data.get("data", [])]
                log(f"SUCCESS: Fetched {len(models)} active model(s): {', '.join(models[:3])}...", "OK")
                return models
    except Exception as e:
        log(f"FAILED to fetch models: {e}", "ERROR")
        return []

def test_metrics_endpoint():
    log("Testing GET /metrics (Observability & Location Data)...")
    try:
        req = Request(f"{PROXY_URL}/metrics", headers={"User-Agent": "OpenCode-Tester/1.0"})
        with urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                data = json.loads(resp.read().decode("utf-8"))
                ip = data.get("verified_public_ip", "Unknown")
                loc = data.get("location", {})
                flag = loc.get("flag", "")
                country = loc.get("country", "")
                log(f"SUCCESS: Verified Public IP: {ip} ({country})", "OK")
                return True
    except Exception as e:
        log(f"FAILED to fetch metrics: {e}", "ERROR")
        return False

def test_completion_request(model_name):
    log(f"Testing POST /v1/chat/completions (Model: {model_name})...")
    payload = {
        "model": model_name,
        "messages": [{"role": "user", "content": "Say hello in 3 words."}],
        "stream": False
    }
    try:
        req = Request(
            f"{PROXY_URL}/v1/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "Authorization": "Bearer public"},
            method="POST"
        )
        with urlopen(req, timeout=30) as resp:
            if resp.status == 200:
                data = json.loads(resp.read().decode("utf-8"))
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                log(f"SUCCESS: Model Response -> '{content.strip()}'", "OK")
                return True
    except Exception as e:
        log(f"FAILED completion request: {e}", "ERROR")
        return False

def test_messages_endpoint(model_name):
    log(f"Testing POST /v1/messages (Anthropic format, Model: {model_name})...")
    payload = {
        "model": model_name,
        "messages": [{"role": "user", "content": "Say hello in 3 words."}],
        "stream": False,
        "max_tokens": 100,
    }
    try:
        req = Request(
            f"{PROXY_URL}/v1/messages",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "x-api-key": "public",
                "anthropic-version": "2023-06-01",
            },
            method="POST",
        )
        with urlopen(req, timeout=30) as resp:
            if resp.status == 200:
                data = json.loads(resp.read().decode("utf-8"))
                content = ""
                if "content" in data:
                    blocks = data["content"]
                    if blocks and isinstance(blocks, list):
                        content = blocks[0].get("text", "")
                log(f"SUCCESS: Anthropic Response -> '{content.strip()}'", "OK")
                return True
    except Exception as e:
        log(f"FAILED messages request: {e}", "ERROR")
        return False

def test_responses_endpoint(model_name):
    log(f"Testing POST /v1/responses (Responses API, Model: {model_name})...")
    payload = {
        "model": model_name,
        "input": "Say hello in 3 words.",
    }
    try:
        req = Request(
            f"{PROXY_URL}/v1/responses",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": "Bearer public",
            },
            method="POST",
        )
        with urlopen(req, timeout=30) as resp:
            if resp.status == 200:
                data = json.loads(resp.read().decode("utf-8"))
                log(f"SUCCESS: Responses API got status 200", "OK")
                return True
    except Exception as e:
        log(f"FAILED responses request: {e}", "ERROR")
        return False

def test_streaming_completion(model_name):
    log(f"Testing POST /v1/chat/completions with stream=True (Model: {model_name})...")
    payload = {
        "model": model_name,
        "messages": [{"role": "user", "content": "Count from 1 to 5."}],
        "stream": True
    }
    try:
        req = Request(
            f"{PROXY_URL}/v1/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "Authorization": "Bearer public"},
            method="POST"
        )
        with urlopen(req, timeout=30) as resp:
            if resp.status == 200:
                chunks = 0
                for line in resp:
                    if line.strip():
                        chunks += 1
                log(f"SUCCESS: Stream completed cleanly with {chunks} stream data lines.", "OK")
                return True
    except Exception as e:
        log(f"FAILED streaming completion request: {e}", "ERROR")
        return False

def main():
    print("=" * 60)
    print("    OpenCode IP Rotator & Proxy Integration Test (test.py)")
    print("=" * 60)
    
    models = test_models_endpoint()
    metrics_ok = test_metrics_endpoint()
    
    if models:
        test_completion_request(models[0])
        test_streaming_completion(models[0])
        test_messages_endpoint(models[0])
        test_responses_endpoint(models[0])
    
    print("=" * 60)
    print(" All Diagnostic Tests Completed!")
    print("=" * 60)

if __name__ == "__main__":
    main()

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from server import app


class RouteTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)

    def test_health_check(self):
        resp = self.client.get("/health")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["status"], "healthy")
        self.assertIn("discovered_models_count", data)

    def test_list_models_format(self):
        resp = self.client.get("/v1/models")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["object"], "list")
        self.assertIsInstance(data["data"], list)

    def test_list_models_returns_all_upstream_model_fields(self):
        model = {
            "id": "big-pickle",
            "object": "model",
            "created": 1793661778,
            "owned_by": "opencode",
            "name": "Big Pickle",
            "type": "chat",
            "limit": {"context": 200000, "output": 32000},
            "capabilities": {"tools": {"supported": True}},
        }

        with patch("routes.models.get_discovered_models", return_value=[model]):
            resp = self.client.get("/v1/models")

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"object": "list", "data": [model]})

    def test_chat_completions_requires_json(self):
        resp = self.client.post(
            "/v1/chat/completions",
            content="not-json",
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(resp.status_code, 400)

    def test_messages_requires_json(self):
        resp = self.client.post(
            "/v1/messages",
            content="not-json",
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(resp.status_code, 400)

    def test_responses_requires_json(self):
        resp = self.client.post(
            "/v1/responses",
            content="not-json",
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(resp.status_code, 400)

    def test_proxy_pool_normalization(self):
        from services.proxy_pool import _normalize_proxy
        self.assertEqual(_normalize_proxy("1.2.3.4:8080"), "http://1.2.3.4:8080")
        self.assertEqual(_normalize_proxy("http://1.2.3.4:8080"), "http://1.2.3.4:8080")
        self.assertEqual(_normalize_proxy("socks5://1.2.3.4:1080"), "socks5://1.2.3.4:1080")

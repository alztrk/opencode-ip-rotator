import unittest
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

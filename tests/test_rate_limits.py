import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import rotator
from services.upstream import classify_upstream_429


class FakeResponse:
    def __init__(self, payload, retry_after=None):
        self._payload = payload
        self.headers = {"retry-after": retry_after} if retry_after else {}
        self.text = "rate limited"

    def json(self):
        return self._payload


class RateLimitClassificationTests(unittest.TestCase):
    def test_quota_preserves_retry_after(self):
        response = FakeResponse({"error": {"type": "FreeUsageLimitError", "message": "quota"}}, "3600")
        category, retry_after, payload = classify_upstream_429(response)
        self.assertEqual(category, "quota")
        self.assertEqual(retry_after, 3600)
        self.assertEqual(payload["error"]["type"], "FreeUsageLimitError")

    def test_general_rate_limit_is_not_misclassified_as_ip_block(self):
        response = FakeResponse({"error": {"type": "RateLimitError", "message": "slow down"}}, "60")
        category, retry_after, _ = classify_upstream_429(response)
        self.assertEqual(category, "rate_limit")
        self.assertEqual(retry_after, 60)

    def test_invalid_retry_after_is_ignored(self):
        response = FakeResponse({"error": {"type": "RateLimitError"}}, "not-a-number")
        _, retry_after, _ = classify_upstream_429(response)
        self.assertIsNone(retry_after)

    def test_unknown_error_type_uses_generic_rate_limit_category(self):
        response = FakeResponse({"error": {"type": "unknown_limit"}})
        category, _, _ = classify_upstream_429(response)
        self.assertEqual(category, "rate_limit")

    def test_missing_lease_table_blocks_rotation_conservatively(self):
        with TemporaryDirectory() as directory:
            original_path = rotator.FLOW_LEASE_DB_PATH
            try:
                rotator.FLOW_LEASE_DB_PATH = Path(directory) / "metrics.db"
                self.assertFalse(rotator.has_active_flow_leases())
                connection = rotator.sqlite3.connect(str(rotator.FLOW_LEASE_DB_PATH))
                connection.close()
                self.assertTrue(rotator.has_active_flow_leases())
            finally:
                rotator.FLOW_LEASE_DB_PATH = original_path


if __name__ == "__main__":
    unittest.main()

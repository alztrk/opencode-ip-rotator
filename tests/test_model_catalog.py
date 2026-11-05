import json
import unittest

from services.discovery import fetch_free_models, is_free_model, parse_free_models


def free_model(model_id="big-pickle", **overrides):
    model = {
        "id": model_id,
        "object": "model",
        "created": 1793661778,
        "owned_by": "opencode",
        "name": "Big Pickle",
        "type": "chat",
        "cost": {
            "text": {
                "input": {"price": 0, "cache": {"read": 0, "write": 0}},
                "output": {"price": 0},
            }
        },
        "api": {
            "chat-completions": {
                "base_url": "https://opencode.ai/inference/openai/v1"
            }
        },
    }
    model.update(overrides)
    return model


def paid_model(model_id="claude-opus-5"):
    model = free_model(model_id)
    model["cost"]["text"]["input"]["price"] = 5
    model["cost"]["text"]["output"]["price"] = 25
    return model


class FakeResponse:
    def __init__(self, body):
        self._body = body.encode("utf-8")

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class FreeModelDetectionTests(unittest.TestCase):
    def test_model_without_free_suffix_is_detected_via_cost(self):
        self.assertTrue(is_free_model(free_model("big-pickle")))

    def test_paid_model_is_not_free(self):
        self.assertFalse(is_free_model(paid_model()))

    def test_model_without_cost_metadata_is_not_free(self):
        model = free_model()
        del model["cost"]
        self.assertFalse(is_free_model(model))

    def test_zero_input_but_paid_output_is_not_free(self):
        model = free_model()
        model["cost"]["text"]["output"]["price"] = 1.2
        self.assertFalse(is_free_model(model))

    def test_boolean_price_is_not_treated_as_zero(self):
        model = free_model()
        model["cost"]["text"]["input"]["price"] = False
        self.assertFalse(is_free_model(model))


class ParseFreeModelsTests(unittest.TestCase):
    def test_only_free_chat_models_are_returned(self):
        payload = {
            "object": "list",
            "data": [
                free_model("big-pickle"),
                paid_model("claude-opus-5"),
                free_model("beam-free"),
            ],
        }
        models = parse_free_models(payload)
        self.assertEqual([m["id"] for m in models], ["big-pickle", "beam-free"])

    def test_all_upstream_model_fields_are_preserved(self):
        source_model = free_model("big-pickle")
        source_model["description"] = "Reasoning model"
        source_model["limit"] = {"context": 200000, "output": 32000}
        source_model["capabilities"] = {"tools": {"supported": True}}
        payload = {"data": [source_model]}
        model = parse_free_models(payload)[0]
        self.assertEqual(model, source_model)

    def test_duplicate_ids_are_removed(self):
        payload = {"data": [free_model("big-pickle"), free_model("big-pickle")]}
        self.assertEqual(len(parse_free_models(payload)), 1)

    def test_invalid_payloads_return_empty_list(self):
        self.assertEqual(parse_free_models(None), [])
        self.assertEqual(parse_free_models({}), [])
        self.assertEqual(parse_free_models({"data": "nope"}), [])


class FetchFreeModelsTests(unittest.TestCase):
    def test_catalog_is_requested_with_public_credentials(self):
        captured = {}
        payload = {"data": [free_model("big-pickle")]}

        def opener(request, timeout):
            captured["url"] = request.full_url
            captured["headers"] = dict(request.header_items())
            captured["timeout"] = timeout
            return FakeResponse(json.dumps(payload))

        models = fetch_free_models(
            "https://example.test/inference/v1/models", timeout=3, opener=opener
        )

        self.assertEqual([m["id"] for m in models], ["big-pickle"])
        self.assertEqual(
            captured["url"], "https://example.test/inference/v1/models"
        )
        self.assertEqual(captured["timeout"], 3)
        headers = {key.lower(): value for key, value in captured["headers"].items()}
        self.assertEqual(headers["authorization"], "Bearer public")

    def test_network_errors_propagate_to_caller(self):
        def opener(request, timeout):
            raise OSError("offline")

        with self.assertRaises(OSError):
            fetch_free_models("https://example.test/models", opener=opener)

    def test_malformed_json_raises(self):
        def opener(request, timeout):
            return FakeResponse("not json")

        with self.assertRaises(json.JSONDecodeError):
            fetch_free_models("https://example.test/models", opener=opener)


if __name__ == "__main__":
    unittest.main()

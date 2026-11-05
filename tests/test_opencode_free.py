import json
import unittest

import services.upstream as free_tier


class FakeResponse:
    def __init__(self, lines, headers=None):
        self._lines = [
            line.encode("utf-8") if isinstance(line, str) else line for line in lines
        ]
        self.headers = headers or {}

    def iter_lines(self):
        yield from self._lines


def sse(payload):
    return "data: " + json.dumps(payload)


class SessionIdTests(unittest.TestCase):
    def test_session_id_matches_opencode_shape(self):
        session_id = free_tier.generate_session_id("conv", 1_700_000_000_000)
        self.assertTrue(session_id.startswith("ses_"))
        self.assertEqual(len(session_id), 30)
        self.assertTrue(free_tier.is_valid_session_id(session_id))

    def test_session_id_encodes_descending_timestamp(self):
        timestamp = 1_700_000_000_000
        session_id = free_tier.generate_session_id("conv", timestamp)
        encoded = int(session_id[4:16], 16)
        expected = (~(timestamp * 0x1000 + 1)) & 0xFFFF_FFFF_FFFF
        self.assertEqual(encoded, expected)

    def test_session_id_is_deterministic_per_seed(self):
        first = free_tier.generate_session_id("conv", 1_700_000_000_000)
        second = free_tier.generate_session_id("conv", 1_700_000_000_000)
        other = free_tier.generate_session_id("other", 1_700_000_000_000)
        self.assertEqual(first, second)
        self.assertNotEqual(first, other)

    def test_invalid_session_ids_are_rejected(self):
        for value in ("", "ses_short", "abc", None, "ses_" + "z" * 26):
            self.assertFalse(free_tier.is_valid_session_id(value))


class HeaderTests(unittest.TestCase):
    def test_headers_identify_the_opencode_cli(self):
        headers = free_tier.opencode_free_headers("ses_0123456789ab" + "A" * 14)
        self.assertTrue(headers["User-Agent"].startswith("opencode/"))
        self.assertEqual(headers["Authorization"], "Bearer public")
        self.assertEqual(headers["Accept"], "text/event-stream")
        self.assertEqual(headers["x-opencode-client"], "cli")
        for name in (
            "x-opencode-session",
            "x-opencode-session-id",
            "x-session-affinity",
            "x-session-id",
        ):
            self.assertTrue(free_tier.is_valid_session_id(headers[name]))

    def test_invalid_client_session_id_is_replaced(self):
        headers = free_tier.opencode_free_headers("not-a-session")
        self.assertTrue(free_tier.is_valid_session_id(headers["x-opencode-session"]))

    def test_project_id_is_optional(self):
        headers = free_tier.opencode_free_headers(project_id="project-1")
        self.assertEqual(headers["x-opencode-project"], "project-1")

    def test_session_id_from_request_headers(self):
        class Headers:
            def __init__(self, values):
                self._values = values

            def get(self, name):
                return self._values.get(name)

        session_id = free_tier.generate_session_id("client", 1_700_000_000_000)
        headers = Headers({"x-opencode-session-id": session_id})
        self.assertEqual(free_tier.session_id_from_headers(headers), session_id)


class ChatShapeTests(unittest.TestCase):
    def test_shape_adds_env_read_tool_and_streaming(self):
        body = {
            "messages": [{"role": "system", "content": "You are helpful."}],
            "stream": False,
        }
        free_tier.apply_free_request_shape(body, "chat")

        content = body["messages"][0]["content"]
        self.assertTrue(
            content.startswith("<env>\n  Working directory: /workspace\n</env>")
        )
        self.assertTrue(content.endswith("You are helpful."))
        self.assertTrue(body["stream"])
        self.assertEqual(body["tool_choice"], "none")
        names = [
            tool.get("function", {}).get("name") for tool in body["tools"]
        ]
        self.assertIn("read", names)

    def test_shape_is_idempotent(self):
        body = {"messages": [{"role": "system", "content": "prompt"}]}
        free_tier.apply_free_request_shape(body, "chat")
        free_tier.apply_free_request_shape(body, "chat")
        self.assertEqual(body["messages"][0]["content"].count("<env>"), 1)

    def test_shape_inserts_system_message_when_missing(self):
        body = {"messages": [{"role": "user", "content": "hi"}]}
        free_tier.apply_free_request_shape(body, "chat")
        self.assertEqual(body["messages"][0]["role"], "system")
        self.assertIn("Working directory:", body["messages"][0]["content"])

    def test_shape_keeps_existing_tools_and_tool_choice(self):
        body = {
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [
                {"type": "function", "function": {"name": "bash", "parameters": {}}}
            ],
            "tool_choice": "auto",
        }
        free_tier.apply_free_request_shape(body, "chat")
        names = [
            tool.get("function", {}).get("name") for tool in body["tools"]
        ]
        self.assertEqual(names, ["bash", "read"])
        self.assertEqual(body["tool_choice"], "auto")

    def test_existing_read_tool_is_not_duplicated(self):
        body = {
            "messages": [{"role": "user", "content": "hi"}],
            "tools": [
                {"type": "function", "function": {"name": "read", "parameters": {}}}
            ],
        }
        free_tier.apply_free_request_shape(body, "chat")
        names = [
            tool.get("function", {}).get("name") for tool in body["tools"]
        ]
        self.assertEqual(names, ["read"])

    def test_real_workspace_is_used_without_compatibility_note(self):
        body = {"messages": [{"role": "system", "content": "prompt"}]}
        free_tier.apply_free_request_shape(
            body, "chat", workspace="/workspace/repository"
        )
        content = body["messages"][0]["content"]
        self.assertTrue(
            content.startswith(
                "<env>\n  Working directory: /workspace/repository\n</env>"
            )
        )
        self.assertNotIn("compatibility marker", content)

    def test_unsafe_workspace_falls_back_to_placeholder(self):
        body = {"messages": [{"role": "system", "content": "prompt"}]}
        free_tier.apply_free_request_shape(body, "chat", workspace="relative/path")
        content = body["messages"][0]["content"]
        self.assertTrue(content.startswith("<env>\n  Working directory: /workspace\n</env>"))
        self.assertIn("compatibility marker", content)


class OtherEndpointShapeTests(unittest.TestCase):
    def test_anthropic_shape_prefixes_system_and_adds_read_tool(self):
        body = {"system": "Anthropic prompt", "stream": False}
        free_tier.apply_free_request_shape(body, "messages")
        self.assertTrue(body["system"].startswith("<env>"))
        self.assertTrue(body["system"].endswith("Anthropic prompt"))
        self.assertTrue(body["stream"])
        self.assertEqual(body["tools"][0]["name"], "read")
        self.assertIn("input_schema", body["tools"][0])
        self.assertEqual(body["tool_choice"], {"type": "none"})

    def test_anthropic_list_system_gets_a_leading_env_block(self):
        body = {"system": [{"type": "text", "text": "prompt"}]}
        free_tier.apply_free_request_shape(body, "messages")
        self.assertEqual(body["system"][0]["type"], "text")
        self.assertIn("Working directory:", body["system"][0]["text"])
        self.assertEqual(body["system"][1]["text"], "prompt")

    def test_responses_shape_uses_instructions_and_top_level_tools(self):
        body = {"instructions": "Responses prompt", "stream": False}
        free_tier.apply_free_request_shape(body, "responses")
        self.assertTrue(body["instructions"].startswith("<env>"))
        self.assertTrue(body["stream"])
        self.assertEqual(body["tools"][0]["name"], "read")
        self.assertEqual(body["tool_choice"], "none")


class ErrorClassificationTests(unittest.TestCase):
    def test_free_tier_error_is_recognized(self):
        payload = {
            "type": "error",
            "error": {
                "type": "FreeTierError",
                "message": "OpenCode's free tier can only be used from within OpenCode",
            },
        }
        self.assertEqual(free_tier.classify_error(payload), "free_tier")
        error = free_tier.error_to_exception(payload)
        self.assertIsNotNone(error)
        self.assertEqual(error.status_code, 403)
        self.assertEqual(error.kind, "free_tier")

    def test_free_usage_limit_is_recognized(self):
        payload = {
            "error": {
                "type": "FreeUsageLimitError",
                "message": "Free usage limit reached.",
            }
        }
        self.assertEqual(free_tier.classify_error(payload), "free_usage_limit")
        error = free_tier.error_to_exception(payload, retry_after=60)
        self.assertEqual(error.status_code, 429)
        self.assertEqual(error.retry_after, 60)

    def test_plain_message_is_recognized(self):
        value = "OpenCode's free tier can only be used from within OpenCode"
        self.assertEqual(free_tier.classify_error(value), "free_tier")

    def test_normal_chunk_is_not_an_error(self):
        payload = {"choices": [{"delta": {"content": "hello"}}]}
        self.assertIsNone(free_tier.classify_error(payload))
        self.assertIsNone(free_tier.error_to_exception(payload))


class StreamAggregationTests(unittest.TestCase):
    def test_chat_stream_is_aggregated(self):
        response = FakeResponse(
            [
                sse({"id": "chat-1", "created": 123, "model": "big-pickle",
                     "choices": [{"delta": {"role": "assistant"}}]}),
                sse({"choices": [{"delta": {"content": "Hello "}}]}),
                sse({"choices": [{"delta": {"content": "world"}, "finish_reason": "stop"}]}),
                sse({"choices": [], "usage": {"prompt_tokens": 3, "completion_tokens": 2}}),
                "data: [DONE]",
            ]
        )
        result = free_tier.collect_chat_completion(response, "big-pickle")
        self.assertEqual(result["id"], "chat-1")
        self.assertEqual(result["created"], 123)
        self.assertEqual(result["choices"][0]["message"]["content"], "Hello world")
        self.assertEqual(result["choices"][0]["finish_reason"], "stop")
        self.assertEqual(result["usage"]["prompt_tokens"], 3)

    def test_chat_stream_aggregates_tool_calls(self):
        response = FakeResponse(
            [
                sse({"choices": [{"delta": {"tool_calls": [
                    {"index": 0, "id": "call_1", "function": {"name": "read", "arguments": "{\"path\":"}}
                ]}}]}),
                sse({"choices": [{"delta": {"tool_calls": [
                    {"index": 0, "function": {"arguments": "\"a.txt\"}"}}
                ]}, "finish_reason": "tool_calls"}]}),
            ]
        )
        result = free_tier.collect_chat_completion(response, "big-pickle")
        message = result["choices"][0]["message"]
        self.assertEqual(message["tool_calls"][0]["id"], "call_1")
        self.assertEqual(message["tool_calls"][0]["function"]["name"], "read")
        self.assertEqual(
            message["tool_calls"][0]["function"]["arguments"], "{\"path\":\"a.txt\"}"
        )

    def test_error_inside_stream_raises(self):
        response = FakeResponse(
            [
                sse({"type": "error", "error": {
                    "type": "FreeTierError",
                    "message": "OpenCode's free tier can only be used from within OpenCode",
                }})
            ]
        )
        with self.assertRaises(free_tier.UpstreamAPIError) as context:
            free_tier.collect_chat_completion(response, "big-pickle")
        self.assertEqual(context.exception.status_code, 403)

    def test_anthropic_stream_is_aggregated(self):
        response = FakeResponse(
            [
                sse({"type": "message_start", "message": {
                    "id": "msg_1", "model": "big-pickle", "role": "assistant",
                    "content": [], "usage": {"input_tokens": 4},
                }}),
                sse({"type": "content_block_start", "index": 0,
                     "content_block": {"type": "text", "text": ""}}),
                sse({"type": "content_block_delta", "index": 0,
                     "delta": {"type": "text_delta", "text": "Hi"}}),
                sse({"type": "content_block_stop", "index": 0}),
                sse({"type": "message_delta", "delta": {"stop_reason": "end_turn"},
                     "usage": {"output_tokens": 1}}),
                sse({"type": "message_stop"}),
            ]
        )
        result = free_tier.collect_anthropic_message(response, "big-pickle")
        self.assertEqual(result["id"], "msg_1")
        self.assertEqual(result["content"][0]["text"], "Hi")
        self.assertEqual(result["stop_reason"], "end_turn")
        self.assertEqual(result["usage"]["output_tokens"], 1)

    def test_responses_stream_uses_completed_object(self):
        response = FakeResponse(
            [
                sse({"type": "response.output_text.delta", "delta": "Hi"}),
                sse({"type": "response.completed", "response": {
                    "id": "resp_1", "object": "response", "status": "completed",
                    "output_text": "Hi", "output": [],
                }}),
            ]
        )
        result = free_tier.collect_responses_api(response, "big-pickle")
        self.assertEqual(result["id"], "resp_1")
        self.assertEqual(result["output_text"], "Hi")

    def test_responses_stream_falls_back_to_a_built_response(self):
        response = FakeResponse(
            [sse({"type": "response.output_text.delta", "delta": "Hi"})]
        )
        result = free_tier.collect_responses_api(response, "big-pickle")
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["output_text"], "Hi")


if __name__ == "__main__":
    unittest.main()

import os
import sys
import types
import unittest
from contextvars import ContextVar
from importlib.util import find_spec

import httpx

os.environ.setdefault("TRACING_SERVICE_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

if "aef_tracing" not in sys.modules and find_spec("aef_tracing") is None:
    aef_tracing = types.ModuleType("aef_tracing")
    exporters = types.ModuleType("aef_tracing.exporters")
    span_processors = types.ModuleType("aef_tracing.span_processors")

    class DummySpan:
        def add_span_attributes(self, **attrs):
            return None

        def add_output_result(self, output=None):
            return None

        def add_response(self, **kwargs):
            return None

    class DummyContextManager:
        def __enter__(self):
            return DummySpan()

        def __exit__(self, exc_type, exc, tb):
            return False

    class DummyProvider:
        def add_span_processor(self, processor):
            return None

        def get_tracer(self, name):
            return object()

    class DummyHandler:
        def __init__(self, *args, **kwargs):
            return None

    class DummySender:
        def __init__(self, *args, **kwargs):
            return None

    def dummy_cm(*args, **kwargs):
        return DummyContextManager()

    aef_tracing.AEFBatchSpanProcessor = DummySender
    aef_tracing.AEFHandler = DummyHandler
    aef_tracing.AEFTracerProvider = DummyProvider
    aef_tracing.aef_agent_start = dummy_cm
    aef_tracing.aef_custom_span = dummy_cm
    aef_tracing.aef_input_request = dummy_cm
    aef_tracing.aef_kafka_consume = dummy_cm
    aef_tracing.aef_kafka_produce = dummy_cm
    aef_tracing.aef_observation = lambda *args, **kwargs: (lambda fn: fn)
    exporters.AEFKafkaSender = DummySender
    exporters.AEFProtobufSenderExporter = DummySender
    span_processors.session_id_cvar = ContextVar("session_id", default=None)

    sys.modules["aef_tracing"] = aef_tracing
    sys.modules["aef_tracing.exporters"] = exporters
    sys.modules["aef_tracing.span_processors"] = span_processors

from app.core import tracing
from app.core.config import settings
from app.core.http_retry import request_with_retry
from app.core.llm_retry import llm_ainvoke_with_retry


class FakeRunnable:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    async def ainvoke(self, messages):
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _http_status_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "https://gigachat.example/v1/chat")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError("GigaChat error", request=request, response=response)


class RetryPolicyTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._old_http = (
            settings.http_retry_base,
            settings.http_retry_max,
            settings.http_retry_jitter,
        )
        self._old_llm = (
            settings.llm_retry_base,
            settings.llm_retry_max,
            settings.llm_retry_jitter,
        )
        settings.http_retry_base = 0
        settings.http_retry_max = 0
        settings.http_retry_jitter = 0
        settings.llm_retry_base = 0
        settings.llm_retry_max = 0
        settings.llm_retry_jitter = 0
        tracing.reset_hops()

    def tearDown(self) -> None:
        (
            settings.http_retry_base,
            settings.http_retry_max,
            settings.http_retry_jitter,
        ) = self._old_http
        (
            settings.llm_retry_base,
            settings.llm_retry_max,
            settings.llm_retry_jitter,
        ) = self._old_llm
        tracing.reset_hops()

    async def test_http_retries_503_then_succeeds(self):
        calls = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            if calls == 1:
                return httpx.Response(503, request=request)
            return httpx.Response(200, json={"ok": True}, request=request)

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://service.example",
        ) as client:
            response = await request_with_retry(
                client,
                "GET",
                "/probe",
                operation_name="test.probe",
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(calls, 2)

    async def test_http_does_not_retry_429(self):
        calls = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            return httpx.Response(429, request=request)

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://service.example",
        ) as client:
            response = await request_with_retry(
                client,
                "GET",
                "/probe",
                operation_name="test.probe",
            )

        self.assertEqual(response.status_code, 429)
        self.assertEqual(calls, 1)

    async def test_llm_retries_500_then_succeeds(self):
        runnable = FakeRunnable([_http_status_error(500), {"ok": True}])

        result = await llm_ainvoke_with_retry(
            runnable,
            [{"role": "user", "content": "ping"}],
            purpose="test_llm",
        )

        self.assertEqual(result, {"ok": True})
        self.assertEqual(runnable.calls, 2)

    async def test_llm_does_not_retry_429(self):
        runnable = FakeRunnable([_http_status_error(429), {"ok": True}])

        with self.assertRaises(httpx.HTTPStatusError):
            await llm_ainvoke_with_retry(
                runnable,
                [{"role": "user", "content": "ping"}],
                purpose="test_llm",
            )

        self.assertEqual(runnable.calls, 1)


if __name__ == "__main__":
    unittest.main()

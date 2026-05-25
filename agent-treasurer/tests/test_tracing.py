import os
import sys
import types
import unittest
import uuid
from contextvars import ContextVar
from importlib.util import find_spec

os.environ.setdefault("TRACING_SERVICE_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

if find_spec("aef_tracing") is None:
    aef_tracing = types.ModuleType("aef_tracing")
    exporters = types.ModuleType("aef_tracing.exporters")
    span_processors = types.ModuleType("aef_tracing.span_processors")

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


class DummySpan:
    def __init__(self) -> None:
        self.attrs = {}
        self.output = None

    def add_span_attributes(self, **attrs):
        self.attrs.update(attrs)

    def add_output_result(self, output=None):
        self.output = output


class TracingHelpersTest(unittest.TestCase):
    def tearDown(self) -> None:
        tracing.reset_hops()

    def test_bind_trace_id_accepts_only_uuid4(self):
        trace_id = str(uuid.uuid4())

        resolved = tracing.bind_x_trace_id(headers=[("x-trace-id", trace_id.encode())])

        self.assertEqual(resolved, trace_id)
        self.assertEqual(tracing.current_x_trace_id(), trace_id)

    def test_invalid_trace_id_is_replaced_with_uuid4(self):
        resolved = tracing.bind_x_trace_id(trace_id="not-a-uuid")

        self.assertEqual(uuid.UUID(resolved).version, 4)

    def test_http_and_kafka_headers_carry_current_trace_id(self):
        trace_id = str(uuid.uuid4())
        tracing.bind_x_trace_id(trace_id=trace_id)

        http_headers = tracing.trace_header_dict({"X-Trace-Id": "old", "X-Test": "1"})
        kafka_headers = dict(tracing.kafka_trace_headers([("x-trace-id", b"old")]))

        self.assertEqual(http_headers["x-trace-id"], trace_id)
        self.assertEqual(http_headers["X-Test"], "1")
        self.assertEqual(kafka_headers["x-trace-id"], trace_id.encode())

    def test_hop_counter_is_request_scoped(self):
        tracing.reset_hops()

        self.assertEqual(tracing.record_hop(), 1)
        self.assertEqual(tracing.record_hop(), 2)
        self.assertEqual(tracing.current_hops(), 2)

    def test_safe_payload_truncates_and_span_attrs_are_sdk_safe(self):
        old_limit = settings.tracing_max_payload_size
        settings.tracing_max_payload_size = 30
        try:
            payload = tracing.safe_trace_json({"blob": "x" * 100})
        finally:
            settings.tracing_max_payload_size = old_limit

        self.assertIn("<truncated>", payload)

        span = DummySpan()
        tracing.safe_span_attributes(span, none_value=None, object_value=object())

        self.assertEqual(span.attrs["none_value"], "")
        self.assertIsInstance(span.attrs["object_value"], str)


if __name__ == "__main__":
    unittest.main()

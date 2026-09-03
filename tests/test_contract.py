"""Contract tests for OKW Kafka Library (stub backend, no broker needed)."""

from __future__ import annotations

import pytest

from okw_kafka.library import OkwKafkaLibrary, _mem_store


@pytest.fixture(autouse=True)
def clear_mem_store():
    _mem_store.clear()
    yield
    _mem_store.clear()


@pytest.fixture
def lib(tmp_path):
    config = tmp_path / "configs"
    config.mkdir()
    (config / "test.yaml").write_text(
        'bootstrap_servers: "localhost:9092"\n'
        'consumer_group: "okw-test"\n'
        'auto_offset_reset: "latest"\n'
        'value_serializer: "json"\n'
        'value_deserializer: "json"\n',
        encoding="utf-8",
    )
    lib = OkwKafkaLibrary(config_dir=str(config), backend="stub")
    lib.kafka_start("test")
    yield lib
    lib.kafka_stop()


# ── Session Lifecycle ───────────────────────────────────────────


class TestSessionLifecycle:

    def test_start_and_stop(self, tmp_path):
        config = tmp_path / "configs"
        config.mkdir()
        (config / "test.yaml").write_text(
            'bootstrap_servers: "localhost:9092"\n', encoding="utf-8"
        )
        lib = OkwKafkaLibrary(config_dir=str(config), backend="stub")
        lib.kafka_start("test")
        assert lib._ctx is not None
        lib.kafka_stop()
        assert lib._ctx is None

    def test_no_session_raises(self):
        lib = OkwKafkaLibrary(backend="stub")
        with pytest.raises(RuntimeError, match="No Kafka session active"):
            lib.kafka_set_topic("test")

    def test_config_not_found(self):
        lib = OkwKafkaLibrary(config_dir="nonexistent", backend="stub")
        with pytest.raises(FileNotFoundError, match="not found"):
            lib.kafka_start("missing")

    def test_path_traversal_rejected(self, lib):
        lib2 = OkwKafkaLibrary(backend="stub")
        with pytest.raises(ValueError, match="Invalid config name"):
            lib2.kafka_start("../etc/passwd")


# ── Produce ─────────────────────────────────────────────────────


class TestProduce:

    def test_set_topic(self, lib):
        lib.kafka_set_topic("my-topic")
        assert lib._ctx._current_topic == "my-topic"

    def test_set_value_auto_type(self, lib):
        lib.kafka_set_value("$.count", "42")
        assert lib._ctx._message_body["count"] == 42

        lib.kafka_set_value("$.active", "true")
        assert lib._ctx._message_body["active"] is True

        lib.kafka_set_value("$.name", "hello")
        assert lib._ctx._message_body["name"] == "hello"

    def test_set_value_as_string(self, lib):
        lib.kafka_set_value_as_string("$.zipcode", "01234")
        assert lib._ctx._message_body["zipcode"] == "01234"

        lib.kafka_set_value_as_string("$.flag", "true")
        assert lib._ctx._message_body["flag"] == "true"

    def test_set_value_nested(self, lib):
        lib.kafka_set_value("$.order.item", "Widget")
        assert lib._ctx._message_body["order"]["item"] == "Widget"

    def test_set_header(self, lib):
        lib.kafka_set_header("correlation-id", "abc-123")
        assert lib._ctx._message_headers["correlation-id"] == "abc-123"

    def test_set_key(self, lib):
        lib.kafka_set_key("order-42")
        assert lib._ctx._message_key == "order-42"

    def test_produce_resets_message(self, lib):
        lib.kafka_set_topic("test-topic")
        lib.kafka_set_value("$.action", "create")
        lib.kafka_set_header("h1", "v1")
        lib.kafka_set_key("k1")
        lib.kafka_produce()

        assert lib._ctx._message_body == {}
        assert lib._ctx._message_headers == {}
        assert lib._ctx._message_key is None

    def test_produce_without_topic_raises(self, lib):
        lib.kafka_set_value("$.x", "1")
        with pytest.raises(RuntimeError, match="No topic set"):
            lib.kafka_produce()

    def test_produce_with_topic_override(self, lib):
        lib.kafka_set_topic("original")
        lib.kafka_set_value("$.x", "1")
        lib.kafka_produce("override-topic")
        assert "override-topic" in lib._ctx._stub_messages


# ── Consume ─────────────────────────────────────────────────────


class TestConsume:

    def test_produce_then_consume(self, lib):
        lib.kafka_set_topic("events")
        lib.kafka_set_value("$.action", "create")
        lib.kafka_set_value("$.item", "Widget-42")
        lib.kafka_set_key("k1")
        lib.kafka_set_header("h1", "v1")
        lib.kafka_produce()

        lib.kafka_consume("events")
        assert lib._ctx._last_message_value == {"action": "create", "item": "Widget-42"}
        assert lib._ctx._last_message_key == "k1"
        assert lib._ctx._last_message_headers == {"h1": "v1"}

    def test_consume_with_filter(self, lib):
        lib.kafka_set_topic("events")

        lib.kafka_set_value("$.item", "A")
        lib.kafka_produce()

        lib.kafka_set_value("$.item", "B")
        lib.kafka_produce()

        lib.kafka_consume("events", "FILTER=$.item=B")
        assert lib._ctx._last_message_value["item"] == "B"

    def test_consume_no_match_raises(self, lib):
        lib.kafka_set_topic("events")
        lib.kafka_set_value("$.item", "A")
        lib.kafka_produce()

        with pytest.raises(RuntimeError, match="No matching message"):
            lib.kafka_consume("events", "FILTER=$.item=Z")

    def test_consume_latest(self, lib):
        lib.kafka_set_topic("events")

        lib.kafka_set_value("$.seq", "1")
        lib.kafka_produce()

        lib.kafka_set_value("$.seq", "2")
        lib.kafka_produce()

        lib.kafka_consume_latest("events")
        assert lib._ctx._last_message_value["seq"] == 2

    def test_consume_empty_topic_raises(self, lib):
        with pytest.raises(RuntimeError):
            lib.kafka_consume("empty-topic")


# ── Verify ──────────────────────────────────────────────────────


class TestVerify:

    def _produce_and_consume(self, lib):
        lib.kafka_set_topic("t")
        lib.kafka_set_value("$.status", "created")
        lib.kafka_set_value("$.id", "12345")
        lib.kafka_set_value("$.msg", "Order created successfully")
        lib.kafka_set_key("order-42")
        lib.kafka_set_header("content-type", "application/json")
        lib.kafka_produce()
        lib.kafka_consume("t")

    def test_verify_value_exact(self, lib):
        self._produce_and_consume(lib)
        lib.kafka_verify_value("$.status", "created")

    def test_verify_value_exact_mismatch(self, lib):
        self._produce_and_consume(lib)
        with pytest.raises(Exception):
            lib.kafka_verify_value("$.status", "deleted")

    def test_verify_value_wcm(self, lib):
        self._produce_and_consume(lib)
        lib.kafka_verify_value_wcm("$.msg", "*successfully")

    def test_verify_value_wcm_mismatch(self, lib):
        self._produce_and_consume(lib)
        with pytest.raises(Exception):
            lib.kafka_verify_value_wcm("$.msg", "*failed*")

    def test_verify_value_regx(self, lib):
        self._produce_and_consume(lib)
        lib.kafka_verify_value_regx("$.id", r"\d{5}")

    def test_verify_value_regx_mismatch(self, lib):
        self._produce_and_consume(lib)
        with pytest.raises(Exception):
            lib.kafka_verify_value_regx("$.id", r"^[a-z]+$")

    def test_verify_header(self, lib):
        self._produce_and_consume(lib)
        lib.kafka_verify_header("content-type", "application/json")

    def test_verify_header_mismatch(self, lib):
        self._produce_and_consume(lib)
        with pytest.raises(Exception):
            lib.kafka_verify_header("content-type", "text/plain")

    def test_verify_key(self, lib):
        self._produce_and_consume(lib)
        lib.kafka_verify_key("order-42")

    def test_verify_key_mismatch(self, lib):
        self._produce_and_consume(lib)
        with pytest.raises(Exception):
            lib.kafka_verify_key("order-99")


# ── Memorize ────────────────────────────────────────────────────


class TestMemorize:

    def test_memorize_value(self, lib):
        lib.kafka_set_topic("t")
        lib.kafka_set_value("$.orderId", "ORD-789")
        lib.kafka_produce()
        lib.kafka_consume("t")

        lib.kafka_memorize_value("$.orderId", "ORDER_ID")
        assert _mem_store["ORDER_ID"] == "ORD-789"

    def test_memorize_key(self, lib):
        lib.kafka_set_topic("t")
        lib.kafka_set_key("my-key")
        lib.kafka_set_value("$.x", "1")
        lib.kafka_produce()
        lib.kafka_consume("t")

        lib.kafka_memorize_key("MSG_KEY")
        assert _mem_store["MSG_KEY"] == "my-key"

    def test_memorize_and_expand(self, lib):
        lib.kafka_set_topic("t")
        lib.kafka_set_value("$.token", "abc-secret")
        lib.kafka_produce()
        lib.kafka_consume("t")

        lib.kafka_memorize_value("$.token", "TOKEN")

        lib.kafka_set_value("$.auth", "$MEM{TOKEN}")
        assert lib._ctx._message_body["auth"] == "abc-secret"


# ── Admin ───────────────────────────────────────────────────────


class TestAdmin:

    def test_verify_topic_exists_yes(self, lib):
        lib.kafka_set_topic("my-topic")
        lib.kafka_set_value("$.x", "1")
        lib.kafka_produce()

        lib.kafka_verify_topic_exists("my-topic", "YES")

    def test_verify_topic_exists_no(self, lib):
        lib.kafka_verify_topic_exists("nonexistent", "NO")

    def test_verify_topic_exists_mismatch(self, lib):
        with pytest.raises(Exception):
            lib.kafka_verify_topic_exists("nonexistent", "YES")

    def test_verify_partition_count(self, lib):
        lib.kafka_set_topic("t")
        lib.kafka_set_value("$.x", "1")
        lib.kafka_produce()
        lib.kafka_verify_partition_count("t", "1")

    def test_log_topics(self, lib):
        lib.kafka_set_topic("t1")
        lib.kafka_set_value("$.x", "1")
        lib.kafka_produce()
        lib.kafka_set_topic("t2")
        lib.kafka_set_value("$.y", "2")
        lib.kafka_produce()
        lib.kafka_log_topics()


# ── Token Tests ─────────────────────────────────────────────────


class TestTokens:

    def test_ignore_set_topic(self, lib):
        lib.kafka_set_topic("$IGNORE")
        assert lib._ctx._current_topic is None

    def test_ignore_set_value(self, lib):
        lib.kafka_set_value("$.x", "$IGNORE")
        assert lib._ctx._message_body == {}

    def test_ignore_set_value_as_string(self, lib):
        lib.kafka_set_value_as_string("$.x", "$IGNORE")
        assert lib._ctx._message_body == {}

    def test_ignore_set_header(self, lib):
        lib.kafka_set_header("h1", "$IGNORE")
        assert lib._ctx._message_headers == {}

    def test_ignore_set_key(self, lib):
        lib.kafka_set_key("$IGNORE")
        assert lib._ctx._message_key is None

    def test_ignore_consume(self, lib):
        lib.kafka_consume("$IGNORE")

    def test_ignore_consume_latest(self, lib):
        lib.kafka_consume_latest("$IGNORE")

    def test_ignore_verify_value(self, lib):
        lib.kafka_verify_value("$.x", "$IGNORE")

    def test_ignore_verify_value_wcm(self, lib):
        lib.kafka_verify_value_wcm("$.x", "$IGNORE")

    def test_ignore_verify_value_regx(self, lib):
        lib.kafka_verify_value_regx("$.x", "$IGNORE")

    def test_ignore_verify_header(self, lib):
        lib.kafka_verify_header("h1", "$IGNORE")

    def test_ignore_verify_key(self, lib):
        lib.kafka_verify_key("$IGNORE")

    def test_ignore_verify_topic_exists(self, lib):
        lib.kafka_verify_topic_exists("$IGNORE")

    def test_ignore_partition_count(self, lib):
        lib.kafka_verify_partition_count("t", "$IGNORE")

    def test_empty_set_value(self, lib):
        lib.kafka_set_value("$.name", "$EMPTY")
        assert lib._ctx._message_body["name"] == ""

    def test_empty_verify_value(self, lib):
        lib.kafka_set_topic("t")
        lib.kafka_set_value_as_string("$.name", "")
        lib.kafka_produce()
        lib.kafka_consume("t")
        lib.kafka_verify_value("$.name", "$EMPTY")

    def test_ignore_kafka_start(self, tmp_path):
        lib = OkwKafkaLibrary(backend="stub")
        lib.kafka_start("$IGNORE")
        assert lib._ctx is None

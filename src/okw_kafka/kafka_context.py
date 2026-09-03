"""KafkaContext — mutable state for one Kafka session."""

from __future__ import annotations

import json
import time
from collections import defaultdict

try:
    import confluent_kafka
    import confluent_kafka.admin
except ImportError:
    confluent_kafka = None


class KafkaContext:
    """Holds producer, consumer, admin client and message build state."""

    def __init__(self, config: dict, backend: str = "confluent"):
        self._config = config
        self._backend = backend

        self._producer = None
        self._consumer = None
        self._admin = None

        self._value_serializer = config.get("value_serializer", "json")
        self._value_deserializer = config.get("value_deserializer", "json")

        self._current_topic: str | None = None
        self._message_body: dict = {}
        self._message_headers: dict[str, str] = {}
        self._message_key: str | None = None

        self._last_message_value: dict | str | None = None
        self._last_message_key: str | None = None
        self._last_message_headers: dict[str, str] = {}

        self._stub_messages: dict[str, list] = defaultdict(list)

        if self._backend == "confluent":
            self._ensure_confluent()

    def _ensure_confluent(self):
        if confluent_kafka is None:
            raise ImportError(
                "confluent-kafka is not installed. "
                "Install it with: pip install confluent-kafka"
            )

    def _build_confluent_config(self, *, consumer: bool = False) -> dict:
        cfg = {"bootstrap.servers": self._config.get("bootstrap_servers", "localhost:9092")}

        if self._config.get("security_protocol"):
            cfg["security.protocol"] = self._config["security_protocol"]
        if self._config.get("sasl_mechanism"):
            cfg["sasl.mechanism"] = self._config["sasl_mechanism"]
        if self._config.get("sasl_username"):
            cfg["sasl.username"] = self._config["sasl_username"]
        if self._config.get("sasl_password"):
            cfg["sasl.password"] = self._config["sasl_password"]
        if self._config.get("ssl_ca_location"):
            cfg["ssl.ca.location"] = self._config["ssl_ca_location"]

        if consumer:
            cfg["group.id"] = self._config.get("consumer_group", "okw-test")
            cfg["auto.offset.reset"] = self._config.get("auto_offset_reset", "latest")
            cfg["enable.auto.commit"] = False

        return cfg

    # ── Producer ────────────────────────────────────────────────

    def _ensure_producer(self):
        if self._backend == "stub":
            return
        if self._producer is None:
            self._producer = confluent_kafka.Producer(self._build_confluent_config())

    def produce(self, topic: str, value: dict | str, key: str | None,
                headers: dict[str, str]):
        serialized = self._serialize(value)
        key_bytes = key.encode("utf-8") if key else None
        header_tuples = [(k, v.encode("utf-8")) for k, v in headers.items()]

        if self._backend == "stub":
            self._stub_messages[topic].append({
                "value": value,
                "key": key,
                "headers": dict(headers),
            })
            return

        self._ensure_producer()
        self._producer.produce(
            topic,
            value=serialized,
            key=key_bytes,
            headers=header_tuples if header_tuples else None,
        )
        self._producer.flush(timeout=10)

    def _serialize(self, value) -> bytes:
        if self._value_serializer == "json":
            return json.dumps(value).encode("utf-8")
        return str(value).encode("utf-8")

    def _deserialize(self, data: bytes):
        if self._value_deserializer == "json":
            return json.loads(data.decode("utf-8"))
        return data.decode("utf-8")

    # ── Consumer ────────────────────────────────────────────────

    def consume(self, topic: str, filter_fn=None, timeout: int = 30):
        if self._backend == "stub":
            return self._stub_consume(topic, filter_fn)

        self._ensure_confluent()
        if self._consumer is None:
            self._consumer = confluent_kafka.Consumer(
                self._build_confluent_config(consumer=True)
            )
        self._consumer.subscribe([topic])

        deadline = time.time() + timeout
        while time.time() < deadline:
            msg = self._consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue

            value = self._deserialize(msg.value())
            if filter_fn and not filter_fn(value):
                continue

            self._store_consumed(value, msg.key(), msg.headers())
            return

        raise RuntimeError(
            f"KafkaConsume: No matching message on '{topic}' within {timeout}s."
        )

    def consume_latest(self, topic: str, timeout: int = 30):
        if self._backend == "stub":
            return self._stub_consume_latest(topic)

        self._ensure_confluent()
        if self._consumer is None:
            self._consumer = confluent_kafka.Consumer(
                self._build_confluent_config(consumer=True)
            )
        self._consumer.subscribe([topic])

        # Seek to end, then poll for new messages
        deadline = time.time() + timeout
        while time.time() < deadline:
            msg = self._consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue

            value = self._deserialize(msg.value())
            self._store_consumed(value, msg.key(), msg.headers())
            return

        raise RuntimeError(
            f"KafkaConsumeLatest: No message on '{topic}' within {timeout}s."
        )

    def _store_consumed(self, value, raw_key, raw_headers):
        self._last_message_value = value
        self._last_message_key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else raw_key
        self._last_message_headers = {}
        if raw_headers:
            for k, v in raw_headers:
                self._last_message_headers[k] = v.decode("utf-8") if isinstance(v, bytes) else str(v)

    def _stub_consume(self, topic: str, filter_fn=None):
        messages = self._stub_messages.get(topic, [])
        for msg in messages:
            if filter_fn and not filter_fn(msg["value"]):
                continue
            self._last_message_value = msg["value"]
            self._last_message_key = msg["key"]
            self._last_message_headers = msg["headers"]
            return
        raise RuntimeError(
            f"KafkaConsume (stub): No matching message on '{topic}'."
        )

    def _stub_consume_latest(self, topic: str):
        messages = self._stub_messages.get(topic, [])
        if not messages:
            raise RuntimeError(
                f"KafkaConsumeLatest (stub): No messages on '{topic}'."
            )
        msg = messages[-1]
        self._last_message_value = msg["value"]
        self._last_message_key = msg["key"]
        self._last_message_headers = msg["headers"]

    # ── Admin ───────────────────────────────────────────────────

    def _ensure_admin(self):
        if self._backend == "stub":
            return
        self._ensure_confluent()
        if self._admin is None:
            self._admin = confluent_kafka.admin.AdminClient(
                self._build_confluent_config()
            )

    def list_topics(self, timeout: float = 10) -> list[str]:
        if self._backend == "stub":
            return sorted(self._stub_messages.keys())

        self._ensure_admin()
        metadata = self._admin.list_topics(timeout=timeout)
        return sorted(metadata.topics.keys())

    def get_topic_partitions(self, topic: str, timeout: float = 10) -> int:
        if self._backend == "stub":
            return 1

        self._ensure_admin()
        metadata = self._admin.list_topics(topic=topic, timeout=timeout)
        if topic not in metadata.topics:
            raise RuntimeError(f"Topic '{topic}' not found.")
        return len(metadata.topics[topic].partitions)

    # ── Value resolution (JSONPath-like) ────────────────────────

    def resolve_value(self, jsonpath: str) -> str:
        if self._last_message_value is None:
            raise RuntimeError("No consumed message available.")
        path = self._strip_jsonpath(jsonpath)
        return str(self._resolve_path(self._last_message_value, path))

    def set_value(self, jsonpath: str, value, force_string: bool = False):
        path = self._strip_jsonpath(jsonpath)
        typed = value if force_string else self._auto_type(value)
        self._set_nested(self._message_body, path, typed)

    @staticmethod
    def _strip_jsonpath(path: str) -> str:
        if path.startswith("$."):
            return path[2:]
        if path.startswith("$"):
            return path[1:]
        return path

    def reset_message(self):
        self._message_body = {}
        self._message_headers = {}
        self._message_key = None

    # ── Helpers (same as RestContext) ────────────────────────────

    @staticmethod
    def _resolve_path(obj, path: str):
        for part in path.split("."):
            if "[" in part:
                key, idx_str = part.split("[", 1)
                idx = int(idx_str.rstrip("]"))
                if key:
                    obj = obj[key]
                obj = obj[idx]
            else:
                obj = obj[part]
        return obj

    @staticmethod
    def _set_nested(target: dict, path: str, value):
        parts = path.split(".")
        for part in parts[:-1]:
            if "[" in part:
                key, idx_str = part.split("[", 1)
                idx = int(idx_str.rstrip("]"))
                if key:
                    target.setdefault(key, [])
                    target = target[key]
                while len(target) <= idx:
                    target.append({})
                target = target[idx]
            else:
                target.setdefault(part, {})
                target = target[part]
        leaf = parts[-1]
        if "[" in leaf:
            key, idx_str = leaf.split("[", 1)
            idx = int(idx_str.rstrip("]"))
            if key:
                target.setdefault(key, [])
                target = target[key]
            while len(target) <= idx:
                target.append(None)
            target[idx] = value
        else:
            target[leaf] = value

    @staticmethod
    def _auto_type(value: str):
        if not isinstance(value, str):
            return value
        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False
        if value.lower() == "null" or value == "$NULL":
            return None
        try:
            return int(value)
        except ValueError:
            pass
        try:
            return float(value)
        except ValueError:
            pass
        return value

    # ── Teardown ────────────────────────────────────────────────

    def close(self):
        if self._consumer:
            try:
                self._consumer.close()
            except Exception:
                pass
            self._consumer = None
        if self._producer:
            try:
                self._producer.flush(timeout=5)
            except Exception:
                pass
            self._producer = None
        self._admin = None

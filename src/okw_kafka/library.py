"""OKW Kafka Library — Robot Framework keyword library for Apache Kafka.

= Overview =

OKW Kafka provides keyword-driven Apache Kafka integration testing.
Produce messages, consume with timeout-based polling, and verify
content using EXACT, WCM and REGX match modes.

Kafka is not the test object — it is the *tool*: trigger asynchronous
processing via messages and verify results in connected systems.

= Keyword Pattern =

The keyword pattern follows the universal OKW model:

| =Phase=       | =Keyword=          | =Purpose=                          |
| Session       | KafkaStart         | Connect via YAML config            |
| Scope         | KafkaSetTopic      | Set target topic                   |
| Input         | KafkaSetValue      | Build message body (JSONPath)      |
| Action        | KafkaProduce       | Send message                       |
| Receive       | KafkaConsume       | Poll until message arrives         |
| Verify        | KafkaVerifyValue   | Check message content              |
| Teardown      | KafkaStop          | Close all connections              |

= YAML Configuration =

```yaml
# configs/kafka-local.yaml
bootstrap_servers: "localhost:9092"
consumer_group: "okw-test"
auto_offset_reset: "latest"
value_serializer: "json"
value_deserializer: "json"
```

= Runnable Examples =

https://github.com/Hrabovszki1023/okw-examples/tree/main/kafka/

= Example =

| KafkaStart          | kafka-local |
| KafkaSetTopic       | order-commands |
| KafkaSetValue       | $.action | create |
| KafkaSetValue       | $.item   | Widget-42 |
| KafkaProduce        |
| KafkaConsume        | order-events | FILTER=$.item=Widget-42 | TIMEOUT=30 |
| KafkaVerifyValue    | $.status | created |
| KafkaStop           |
"""

from __future__ import annotations

import os
import re
import yaml

from robot.api import logger
from robot.api.deco import keyword, library

from okw_contract_utils import MatchMode, assert_match, expand_mem
from okw_contract_utils.tokens import is_ignore, is_empty_token, parse_yes_no, assert_exists

from .kafka_context import KafkaContext


_mem_store: dict[str, str] = {}

_FILTER_RE = re.compile(r"^FILTER=(.+)$", re.IGNORECASE)
_TIMEOUT_RE = re.compile(r"^TIMEOUT=(\d+)$", re.IGNORECASE)


def _expand(value: str) -> str:
    if value is None:
        return ""
    return expand_mem(str(value), _mem_store)


def _resolve_empty(value: str) -> str:
    if is_empty_token(value):
        return ""
    return value


@library(scope="GLOBAL", version="0.1.1")
class OkwKafkaLibrary:
    """OKW Kafka Library for Robot Framework.

    See module docstring for full documentation.
    """

    ROBOT_LIBRARY_SCOPE = "GLOBAL"
    ROBOT_LIBRARY_DOC_FORMAT = "ROBOT"
    ROBOT_LIBRARY_VERSION = "0.1.1"

    def __init__(self, config_dir: str = "configs", backend: str = "stub"):
        self._config_dir = config_dir
        self._backend = backend.lower().strip()
        self._ctx: KafkaContext | None = None

    def _require_ctx(self) -> KafkaContext:
        if self._ctx is None:
            raise RuntimeError("No Kafka session active. Call KafkaStart first.")
        return self._ctx

    def _load_config(self, name: str) -> dict:
        if ".." in name or "/" in name or "\\" in name:
            raise ValueError(f"Invalid config name: '{name}'")

        search_dirs = []

        try:
            from robot.libraries.BuiltIn import BuiltIn
            suite_source = BuiltIn().get_variable_value("${SUITE SOURCE}")
            if suite_source:
                search_dirs.append(os.path.join(os.path.dirname(suite_source), self._config_dir))
                search_dirs.append(os.path.dirname(suite_source))
        except Exception:
            pass

        search_dirs.append(os.path.join(os.getcwd(), self._config_dir))
        search_dirs.append(os.getcwd())

        for d in search_dirs:
            for ext in (".yaml", ".yml"):
                path = os.path.join(d, f"{name}{ext}")
                if os.path.isfile(path):
                    with open(path, encoding="utf-8") as f:
                        return yaml.safe_load(f)

        raise FileNotFoundError(
            f"Kafka config '{name}' not found. Searched: {search_dirs}"
        )

    # ── Session Lifecycle ───────────���───────────────────────────

    @keyword("KafkaStart")
    def kafka_start(self, config: str):
        """Connect to a Kafka broker via YAML configuration.

        ``config`` is the name of a YAML file in the configs directory
        (without extension).

        Example:
        | KafkaStart | kafka-local |
        """
        expanded = _expand(config)
        if is_ignore(expanded):
            logger.info("KafkaStart: IGNORED")
            return

        cfg = self._load_config(expanded)
        self._ctx = KafkaContext(cfg, backend=self._backend)

        broker = cfg.get("bootstrap_servers", "localhost:9092")
        group = cfg.get("consumer_group", "okw-test")
        logger.info(f"KafkaStart: Connected to '{broker}' (group='{group}', backend='{self._backend}')")

    @keyword("KafkaStop")
    def kafka_stop(self):
        """Close all Kafka producers and consumers.

        Example:
        | KafkaStop |
        """
        if self._ctx is not None:
            self._ctx.close()
        self._ctx = None
        logger.info("KafkaStop: Session closed.")

    # ── Produce ─────────────────────────────────────────────────

    @keyword("KafkaSetTopic")
    def kafka_set_topic(self, topic: str):
        """Set the target topic for the next produce or consume.

        Example:
        | KafkaSetTopic | order-commands |
        """
        expanded = _expand(topic)
        if is_ignore(expanded):
            logger.info("KafkaSetTopic: IGNORED")
            return
        ctx = self._require_ctx()
        ctx._current_topic = expanded
        logger.info(f"KafkaSetTopic: '{expanded}'")

    @keyword("KafkaSetValue")
    def kafka_set_value(self, field: str, value: str):
        """Set a field in the message body (JSONPath notation).

        Values are auto-typed: ``42`` becomes integer, ``true`` becomes
        boolean, ``null`` becomes None. Use ``KafkaSetValueAsString``
        to force string type.

        Example:
        | KafkaSetValue | $.action | create |
        | KafkaSetValue | $.item   | Widget-42 |
        """
        expanded = _expand(value)
        if is_ignore(expanded):
            logger.info(f"KafkaSetValue {field}: IGNORED")
            return
        expanded = _resolve_empty(expanded)
        ctx = self._require_ctx()
        ctx.set_value(field, expanded, force_string=False)
        logger.info(f"KafkaSetValue: {field} = '{expanded}'")

    @keyword("KafkaSetValueAsString")
    def kafka_set_value_as_string(self, field: str, value: str):
        """Set a field in the message body, always as string (no auto-conversion).

        Example:
        | KafkaSetValueAsString | $.zipcode | 01234 |
        """
        expanded = _expand(value)
        if is_ignore(expanded):
            logger.info(f"KafkaSetValueAsString {field}: IGNORED")
            return
        expanded = _resolve_empty(expanded)
        ctx = self._require_ctx()
        ctx.set_value(field, expanded, force_string=True)
        logger.info(f"KafkaSetValueAsString: {field} = '{expanded}'")

    @keyword("KafkaSetHeader")
    def kafka_set_header(self, key: str, value: str):
        """Set a Kafka message header.

        Example:
        | KafkaSetHeader | correlation-id | abc-123 |
        """
        expanded_val = _expand(value)
        if is_ignore(expanded_val):
            logger.info(f"KafkaSetHeader {key}: IGNORED")
            return
        expanded_val = _resolve_empty(expanded_val)
        ctx = self._require_ctx()
        ctx._message_headers[_expand(key)] = expanded_val
        logger.info(f"KafkaSetHeader: {key} = '{expanded_val}'")

    @keyword("KafkaSetKey")
    def kafka_set_key(self, key: str):
        """Set the message key (used for partitioning).

        Example:
        | KafkaSetKey | order-42 |
        """
        expanded = _expand(key)
        if is_ignore(expanded):
            logger.info("KafkaSetKey: IGNORED")
            return
        expanded = _resolve_empty(expanded)
        ctx = self._require_ctx()
        ctx._message_key = expanded
        logger.info(f"KafkaSetKey: '{expanded}'")

    @keyword("KafkaProduce")
    def kafka_produce(self, topic: str = None):
        """Send the prepared message to Kafka.

        Uses the topic from ``KafkaSetTopic`` unless overridden here.
        After sending, the message body, headers, and key are reset.

        Example:
        | KafkaProduce |
        | KafkaProduce | order-commands |
        """
        ctx = self._require_ctx()
        resolved_topic = _expand(topic) if topic else ctx._current_topic
        if not resolved_topic:
            raise RuntimeError("No topic set. Call KafkaSetTopic or pass topic to KafkaProduce.")

        ctx.produce(
            resolved_topic,
            ctx._message_body,
            ctx._message_key,
            ctx._message_headers,
        )
        body_summary = str(ctx._message_body)[:200]
        logger.info(f"KafkaProduce: topic='{resolved_topic}', key='{ctx._message_key}', body={body_summary}")
        ctx.reset_message()

    # ── Consume ──���──────────────────────────────────────────────

    @keyword("KafkaConsume")
    def kafka_consume(self, topic: str, *args):
        """Consume a message from the topic with timeout-based polling.

        Polls until a matching message arrives or the timeout expires.
        Optional named arguments: ``FILTER=$.field=value``, ``TIMEOUT=30``.

        Example:
        | KafkaConsume | order-events |
        | KafkaConsume | order-events | FILTER=$.item=Widget-42 | TIMEOUT=30 |
        """
        expanded_topic = _expand(topic)
        if is_ignore(expanded_topic):
            logger.info("KafkaConsume: IGNORED")
            return

        filter_fn = None
        timeout = 30

        for arg in args:
            arg_str = _expand(str(arg))
            m_filter = _FILTER_RE.match(arg_str)
            m_timeout = _TIMEOUT_RE.match(arg_str)
            if m_filter:
                filter_fn = self._build_filter(m_filter.group(1))
            elif m_timeout:
                timeout = int(m_timeout.group(1))

        ctx = self._require_ctx()
        ctx.consume(expanded_topic, filter_fn, timeout)
        logger.info(f"KafkaConsume: Received message on '{expanded_topic}'")

    @keyword("KafkaConsumeLatest")
    def kafka_consume_latest(self, topic: str, timeout: int = 30):
        """Consume the latest message from the topic, skipping backlog.

        Example:
        | KafkaConsumeLatest | order-events |
        | KafkaConsumeLatest | order-events | TIMEOUT=30 |
        """
        expanded_topic = _expand(topic)
        if is_ignore(expanded_topic):
            logger.info("KafkaConsumeLatest: IGNORED")
            return
        ctx = self._require_ctx()
        ctx.consume_latest(expanded_topic, int(timeout))
        logger.info(f"KafkaConsumeLatest: Received message on '{expanded_topic}'")

    @staticmethod
    def _build_filter(expression: str):
        parts = expression.split("=", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid filter expression: '{expression}'. Expected: $.field=value")
        field_path = parts[0].strip()
        expected_value = parts[1].strip()

        def _filter(msg_value):
            try:
                actual = str(KafkaContext._resolve_path(
                    msg_value, KafkaContext._strip_jsonpath(field_path)
                ))
                return actual == expected_value
            except (KeyError, IndexError, TypeError):
                return False

        return _filter

    # ── Verify ─────────────────���────────────────────────────────

    @keyword("KafkaVerifyValue")
    def kafka_verify_value(self, jsonpath: str, expected: str):
        """Verify a field in the consumed message (EXACT match).

        Example:
        | KafkaVerifyValue | $.status | created |
        """
        exp = _expand(expected)
        if is_ignore(exp):
            logger.info(f"KafkaVerifyValue {jsonpath}: IGNORED")
            return
        exp = _resolve_empty(exp)
        ctx = self._require_ctx()
        actual = ctx.resolve_value(jsonpath)
        assert_match(actual, exp, MatchMode.EXACT,
                     context=f"KafkaVerifyValue: {jsonpath}")
        logger.info(f"KafkaVerifyValue: {jsonpath} = '{actual}' (PASS)")

    @keyword("KafkaVerifyValueWCM")
    def kafka_verify_value_wcm(self, jsonpath: str, pattern: str):
        """Verify a field in the consumed message (Wildcard match).

        ``*`` matches any characters, ``?`` matches one character.

        Example:
        | KafkaVerifyValueWCM | $.message | *success* |
        """
        exp = _expand(pattern)
        if is_ignore(exp):
            logger.info(f"KafkaVerifyValueWCM {jsonpath}: IGNORED")
            return
        ctx = self._require_ctx()
        actual = ctx.resolve_value(jsonpath)
        assert_match(actual, exp, MatchMode.WCM,
                     context=f"KafkaVerifyValueWCM: {jsonpath}")
        logger.info(f"KafkaVerifyValueWCM: {jsonpath} = '{actual}' (PASS)")

    @keyword("KafkaVerifyValueREGX")
    def kafka_verify_value_regx(self, jsonpath: str, regex: str):
        """Verify a field in the consumed message (Regex match).

        Example:
        | KafkaVerifyValueREGX | $.id | \\d{4,} |
        """
        exp = _expand(regex)
        if is_ignore(exp):
            logger.info(f"KafkaVerifyValueREGX {jsonpath}: IGNORED")
            return
        ctx = self._require_ctx()
        actual = ctx.resolve_value(jsonpath)
        assert_match(actual, exp, MatchMode.REGX,
                     context=f"KafkaVerifyValueREGX: {jsonpath}")
        logger.info(f"KafkaVerifyValueREGX: {jsonpath} = '{actual}' (PASS)")

    @keyword("KafkaVerifyHeader")
    def kafka_verify_header(self, key: str, expected: str):
        """Verify a Kafka message header value.

        Example:
        | KafkaVerifyHeader | content-type | application/json |
        """
        exp = _expand(expected)
        if is_ignore(exp):
            logger.info(f"KafkaVerifyHeader {key}: IGNORED")
            return
        exp = _resolve_empty(exp)
        ctx = self._require_ctx()
        expanded_key = _expand(key)
        actual = ctx._last_message_headers.get(expanded_key, "")
        assert_match(actual, exp, MatchMode.EXACT,
                     context=f"KafkaVerifyHeader: {expanded_key}")
        logger.info(f"KafkaVerifyHeader: {expanded_key} = '{actual}' (PASS)")

    @keyword("KafkaVerifyKey")
    def kafka_verify_key(self, expected: str):
        """Verify the message key.

        Example:
        | KafkaVerifyKey | order-42 |
        """
        exp = _expand(expected)
        if is_ignore(exp):
            logger.info("KafkaVerifyKey: IGNORED")
            return
        exp = _resolve_empty(exp)
        ctx = self._require_ctx()
        actual = ctx._last_message_key or ""
        assert_match(actual, exp, MatchMode.EXACT,
                     context="KafkaVerifyKey")
        logger.info(f"KafkaVerifyKey: '{actual}' (PASS)")

    # ── Memorize ────────────────────────────────────────────────

    @keyword("KafkaMemorizeValue")
    def kafka_memorize_value(self, jsonpath: str, key: str):
        """Store a field value from the consumed message in $MEM{KEY}.

        Example:
        | KafkaMemorizeValue | $.orderId | ORDER_ID |
        """
        ctx = self._require_ctx()
        value = ctx.resolve_value(jsonpath)
        _mem_store[key] = value
        logger.info(f"KafkaMemorizeValue: {jsonpath} -> $MEM{{{key}}} = '{value}'")

    @keyword("KafkaMemorizeKey")
    def kafka_memorize_key(self, key: str):
        """Store the message key in $MEM{KEY}.

        Example:
        | KafkaMemorizeKey | MSG_KEY |
        """
        ctx = self._require_ctx()
        value = ctx._last_message_key or ""
        _mem_store[key] = value
        logger.info(f"KafkaMemorizeKey: -> $MEM{{{key}}} = '{value}'")

    # ── Admin ────────��───────────────────────────���──────────────

    @keyword("KafkaVerifyTopicExists")
    def kafka_verify_topic_exists(self, topic: str, expected: str = "YES"):
        """Verify that a Kafka topic exists (YES) or does not exist (NO).

        Example:
        | KafkaVerifyTopicExists | order-events | YES |
        """
        expanded = _expand(topic)
        if is_ignore(expanded):
            logger.info("KafkaVerifyTopicExists: IGNORED")
            return
        yes_no = parse_yes_no(expected)
        ctx = self._require_ctx()
        ctx._ensure_admin()
        topics = ctx.list_topics()
        exists = expanded in topics
        assert_exists(exists, yes_no,
                      context=f"KafkaVerifyTopicExists: {expanded}")
        logger.info(f"KafkaVerifyTopicExists: '{expanded}' exists={exists} (PASS)")

    @keyword("KafkaVerifyPartitionCount")
    def kafka_verify_partition_count(self, topic: str, expected: str):
        """Verify the number of partitions for a topic.

        Example:
        | KafkaVerifyPartitionCount | order-events | 3 |
        """
        exp = _expand(expected)
        if is_ignore(exp):
            logger.info("KafkaVerifyPartitionCount: IGNORED")
            return
        expanded_topic = _expand(topic)
        ctx = self._require_ctx()
        ctx._ensure_admin()
        actual = ctx.get_topic_partitions(expanded_topic)
        if int(actual) != int(exp):
            raise AssertionError(
                f"KafkaVerifyPartitionCount: topic '{expanded_topic}' "
                f"has {actual} partitions, expected {exp}."
            )
        logger.info(f"KafkaVerifyPartitionCount: '{expanded_topic}' = {actual} (PASS)")

    @keyword("KafkaLogTopics")
    def kafka_log_topics(self):
        """Log all available Kafka topics.

        Example:
        | KafkaLogTopics |
        """
        ctx = self._require_ctx()
        ctx._ensure_admin()
        topics = ctx.list_topics()
        logger.info("KafkaLogTopics:")
        for t in topics:
            logger.info(f"  {t}")
        logger.info(f"Total: {len(topics)} topics")

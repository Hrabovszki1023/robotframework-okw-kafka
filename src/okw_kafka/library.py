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

from robot.api.deco import keyword, library


@library(scope="GLOBAL", version="0.1.0")
class OkwKafkaLibrary:
    """OKW Kafka Library for Robot Framework.

    See module docstring for full documentation.
    """

    ROBOT_LIBRARY_SCOPE = "GLOBAL"

    def __init__(self):
        self._sessions = {}
        self._current_topic = None
        self._message_body = {}
        self._message_headers = {}
        self._message_key = None
        self._last_consumed = None

    # --- Session Lifecycle ---

    @keyword("KafkaStart")
    def kafka_start(self, config: str):
        """Connect to a Kafka broker via YAML configuration.

        ``config`` is the name of a YAML file in ``configs/`` (without extension).

        Example:
        | KafkaStart | kafka-local |
        """
        raise NotImplementedError("KafkaStart not yet implemented")

    @keyword("KafkaStop")
    def kafka_stop(self):
        """Close all Kafka producers and consumers.

        Example:
        | KafkaStop |
        """
        raise NotImplementedError("KafkaStop not yet implemented")

    # --- Produce ---

    @keyword("KafkaSetTopic")
    def kafka_set_topic(self, topic: str):
        """Set the target topic for the next produce or consume.

        Example:
        | KafkaSetTopic | order-commands |
        """
        self._current_topic = topic

    @keyword("KafkaSetValue")
    def kafka_set_value(self, field: str, value: str):
        """Set a field in the message body (JSONPath notation).

        Example:
        | KafkaSetValue | $.action | create |
        | KafkaSetValue | $.item   | Widget-42 |
        """
        raise NotImplementedError("KafkaSetValue not yet implemented")

    @keyword("KafkaSetValueAsString")
    def kafka_set_value_as_string(self, field: str, value: str):
        """Set a field in the message body, always as string (no auto-conversion).

        Example:
        | KafkaSetValueAsString | $.zipcode | 01234 |
        """
        raise NotImplementedError("KafkaSetValueAsString not yet implemented")

    @keyword("KafkaSetHeader")
    def kafka_set_header(self, key: str, value: str):
        """Set a Kafka message header.

        Example:
        | KafkaSetHeader | correlation-id | abc-123 |
        """
        self._message_headers[key] = value

    @keyword("KafkaSetKey")
    def kafka_set_key(self, key: str):
        """Set the message key (used for partitioning).

        Example:
        | KafkaSetKey | order-42 |
        """
        self._message_key = key

    @keyword("KafkaProduce")
    def kafka_produce(self, topic: str = None):
        """Send the prepared message to Kafka.

        Uses the topic from ``KafkaSetTopic`` unless overridden here.

        Example:
        | KafkaProduce |
        | KafkaProduce | order-commands |
        """
        raise NotImplementedError("KafkaProduce not yet implemented")

    # --- Consume ---

    @keyword("KafkaConsume")
    def kafka_consume(self, topic: str, filter: str = None, timeout: int = 30):
        """Consume a message from the topic with timeout-based polling.

        Polls until a matching message arrives or the timeout expires.
        ``filter`` is an optional JSONPath filter (e.g. ``$.item=Widget-42``).

        Example:
        | KafkaConsume | order-events | FILTER=$.item=Widget-42 | TIMEOUT=30 |
        """
        raise NotImplementedError("KafkaConsume not yet implemented")

    @keyword("KafkaConsumeLatest")
    def kafka_consume_latest(self, topic: str, timeout: int = 30):
        """Consume the latest message from the topic, skipping backlog.

        Example:
        | KafkaConsumeLatest | order-events | TIMEOUT=30 |
        """
        raise NotImplementedError("KafkaConsumeLatest not yet implemented")

    # --- Verify ---

    @keyword("KafkaVerifyValue")
    def kafka_verify_value(self, jsonpath: str, expected: str):
        """Verify a field in the consumed message (EXACT match).

        Example:
        | KafkaVerifyValue | $.status | created |
        """
        raise NotImplementedError("KafkaVerifyValue not yet implemented")

    @keyword("KafkaVerifyValueWCM")
    def kafka_verify_value_wcm(self, jsonpath: str, pattern: str):
        """Verify a field in the consumed message (Wildcard match).

        Example:
        | KafkaVerifyValueWCM | $.message | *success* |
        """
        raise NotImplementedError("KafkaVerifyValueWCM not yet implemented")

    @keyword("KafkaVerifyValueREGX")
    def kafka_verify_value_regx(self, jsonpath: str, regex: str):
        """Verify a field in the consumed message (Regex match).

        Example:
        | KafkaVerifyValueREGX | $.id | \\d{4,} |
        """
        raise NotImplementedError("KafkaVerifyValueREGX not yet implemented")

    @keyword("KafkaVerifyHeader")
    def kafka_verify_header(self, key: str, expected: str):
        """Verify a Kafka message header value.

        Example:
        | KafkaVerifyHeader | content-type | application/json |
        """
        raise NotImplementedError("KafkaVerifyHeader not yet implemented")

    @keyword("KafkaVerifyKey")
    def kafka_verify_key(self, expected: str):
        """Verify the message key.

        Example:
        | KafkaVerifyKey | order-42 |
        """
        raise NotImplementedError("KafkaVerifyKey not yet implemented")

    # --- Memorize ---

    @keyword("KafkaMemorizeValue")
    def kafka_memorize_value(self, jsonpath: str, key: str):
        """Store a field value from the consumed message in $MEM{KEY}.

        Example:
        | KafkaMemorizeValue | $.orderId | ORDER_ID |
        """
        raise NotImplementedError("KafkaMemorizeValue not yet implemented")

    @keyword("KafkaMemorizeKey")
    def kafka_memorize_key(self, key: str):
        """Store the message key in $MEM{KEY}.

        Example:
        | KafkaMemorizeKey | MSG_KEY |
        """
        raise NotImplementedError("KafkaMemorizeKey not yet implemented")

    # --- Admin ---

    @keyword("KafkaVerifyTopicExists")
    def kafka_verify_topic_exists(self, topic: str, expected: str = "YES"):
        """Verify that a Kafka topic exists (YES) or does not exist (NO).

        Example:
        | KafkaVerifyTopicExists | order-events | YES |
        """
        raise NotImplementedError("KafkaVerifyTopicExists not yet implemented")

    @keyword("KafkaVerifyPartitionCount")
    def kafka_verify_partition_count(self, topic: str, expected: str):
        """Verify the number of partitions for a topic.

        Example:
        | KafkaVerifyPartitionCount | order-events | 3 |
        """
        raise NotImplementedError("KafkaVerifyPartitionCount not yet implemented")

    @keyword("KafkaLogTopics")
    def kafka_log_topics(self):
        """Log all available Kafka topics.

        Example:
        | KafkaLogTopics |
        """
        raise NotImplementedError("KafkaLogTopics not yet implemented")

# robotframework-okw-kafka

[![PyPI](https://img.shields.io/pypi/v/robotframework-okw-kafka.svg)](https://pypi.org/project/robotframework-okw-kafka/)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

Robot Framework keyword library for Apache Kafka integration testing —
produce, consume, and verify messages using the OKW keyword pattern.

> Deutsche Version: [README_de.md](README_de.md)

## Signal vs. NOISE

| Signal (your test) | NOISE (hidden in YAML) |
|---|---|
| `KafkaSetValue $.action create` | Bootstrap servers, serializers |
| `KafkaConsume order-events TIMEOUT=30` | Consumer group, offset config |
| `KafkaVerifyValue $.status completed` | SSL, authentication, partitions |

## Install

```bash
pip install robotframework-okw-kafka
```

## Minimal Example

```robot
*** Settings ***
Library    okw_kafka.library.OkwKafkaLibrary

*** Test Cases ***
Order Event Roundtrip
    KafkaStart          kafka-local
    KafkaSetTopic       order-commands
    KafkaSetValue       $.action       create
    KafkaSetValue       $.item         Widget-42
    KafkaProduce

    KafkaConsume        order-events    FILTER=$.item=Widget-42    TIMEOUT=30
    KafkaVerifyValue    $.status        created
    KafkaMemorizeValue  $.orderId       ORDER_ID

    KafkaStop
```

## Keyword Pattern

The same pattern across all OKW technologies:

| Phase | GUI | REST | SSH | Kafka |
|---|---|---|---|---|
| Scope | `SelectWindow` | `RESTSelectEndpoint` | `Open Remote Session` | `KafkaSetTopic` |
| Input | `SetValue` | `RESTSetValue` | `Set Remote` | `KafkaSetValue` |
| Action | `ClickOn` | `RESTSendRequest` | `Execute Remote` | `KafkaProduce` |
| Verify | `VerifyValue` | `RESTVerifyValue` | `Verify Remote Response` | `KafkaVerifyValue` |

## YAML Configuration

```yaml
# configs/kafka-local.yaml
bootstrap_servers: "localhost:9092"
consumer_group: "okw-test"
auto_offset_reset: "latest"
value_serializer: "json"
value_deserializer: "json"
```

## Keywords

### Session
- `KafkaStart` / `KafkaStop`

### Produce
- `KafkaSetTopic`, `KafkaSetValue`, `KafkaSetValueAsString`
- `KafkaSetHeader`, `KafkaSetKey`, `KafkaProduce`

### Consume
- `KafkaConsume` (timeout-based polling, optional JSONPath filter)
- `KafkaConsumeLatest`

### Verify
- `KafkaVerifyValue` / `KafkaVerifyValueWCM` / `KafkaVerifyValueREGX`
- `KafkaVerifyHeader`, `KafkaVerifyKey`

### Memorize
- `KafkaMemorizeValue`, `KafkaMemorizeKey`

### Admin
- `KafkaVerifyTopicExists`, `KafkaVerifyPartitionCount`, `KafkaLogTopics`

## Keyword Documentation

[Libdoc (GitHub Pages)](https://hrabovszki1023.github.io/robotframework-okw-kafka/)

## Runnable Examples

[okw-examples/kafka/](https://github.com/Hrabovszki1023/okw-examples/tree/main/kafka/)

## License

AGPL-3.0 — see [LICENSE](LICENSE).

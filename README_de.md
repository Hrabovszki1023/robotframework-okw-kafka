# robotframework-okw-kafka

[![PyPI](https://img.shields.io/pypi/v/robotframework-okw-kafka.svg)](https://pypi.org/project/robotframework-okw-kafka/)
[![Python](https://img.shields.io/pypi/pyversions/robotframework-okw-kafka)](https://pypi.org/project/robotframework-okw-kafka/)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

Robot Framework Schluesselwort-Bibliothek fuer Apache Kafka Integrationstests —
Messages senden, empfangen und pruefen mit dem OKW-Keyword-Muster.

> English version: [README.md](README.md)

## Signal vs. NOISE

| Signal (dein Test) | NOISE (versteckt in YAML) |
|---|---|
| `KafkaSetValue $.action create` | Bootstrap-Server, Serializer |
| `KafkaConsume order-events TIMEOUT=30` | Consumer-Group, Offset-Config |
| `KafkaVerifyValue $.status completed` | SSL, Authentifizierung, Partitionen |

## Installation

```bash
pip install robotframework-okw-kafka
```

## Minimalbeispiel

```robot
*** Settings ***
Library    okw_kafka.library.OkwKafkaLibrary

*** Test Cases ***
Bestell-Event Roundtrip
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

## Keyword-Muster

Dasselbe Muster ueber alle OKW-Technologien:

| Phase | GUI | REST | SSH | Kafka |
|---|---|---|---|---|
| Scope | `SelectWindow` | `RESTSelectEndpoint` | `Open Remote Session` | `KafkaSetTopic` |
| Input | `SetValue` | `RESTSetValue` | `Set Remote` | `KafkaSetValue` |
| Aktion | `ClickOn` | `RESTSendRequest` | `Execute Remote` | `KafkaProduce` |
| Pruefen | `VerifyValue` | `RESTVerifyValue` | `Verify Remote Response` | `KafkaVerifyValue` |

## Lizenz

AGPL-3.0 — siehe [LICENSE](LICENSE).

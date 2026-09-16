# robotframework-okw-kafka

[![PyPI](https://img.shields.io/pypi/v/robotframework-okw-kafka.svg)](https://pypi.org/project/robotframework-okw-kafka/)
[![Python](https://img.shields.io/pypi/pyversions/robotframework-okw-kafka)](https://pypi.org/project/robotframework-okw-kafka/)
[![License](https://img.shields.io/badge/License-OKW_Community-orange.svg)](LICENSE)

Robot Framework Keyword-Bibliothek für Apache Kafka Integrationstests —
Messages senden, empfangen und prüfen mit dem OKW-Keyword-Muster.

> **English version:** [README.md](README.md)

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

Dasselbe Muster über alle OKW-Technologien:

| Phase | GUI | REST | SSH | Kafka |
|---|---|---|---|---|
| Scope | `SelectWindow` | `RESTSelectEndpoint` | `Open Remote Session` | `KafkaSetTopic` |
| Input | `SetValue` | `RESTSetValue` | `Set Remote` | `KafkaSetValue` |
| Aktion | `ClickOn` | `RESTSendRequest` | `Execute Remote` | `KafkaProduce` |
| Prüfen | `VerifyValue` | `RESTVerifyValue` | `Verify Remote Response` | `KafkaVerifyValue` |

## YAML-Konfiguration

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
- `KafkaConsume` (Timeout-basiertes Polling, optionaler JSONPath-Filter)
- `KafkaConsumeLatest`

### Verify
- `KafkaVerifyValue` / `KafkaVerifyValueWCM` / `KafkaVerifyValueREGX`
- `KafkaVerifyHeader`, `KafkaVerifyKey`

### Memorize
- `KafkaMemorizeValue`, `KafkaMemorizeKey`

### Admin
- `KafkaVerifyTopicExists`, `KafkaVerifyPartitionCount`, `KafkaLogTopics`

## Keyword-Dokumentation

[Libdoc (GitHub Pages)](https://hrabovszki1023.github.io/robotframework-okw-kafka/)

## Lauffähige Beispiele

[okw-examples/kafka/](https://github.com/Hrabovszki1023/okw-examples/tree/main/kafka/) — Produce, Consume, Verify, Admin-Keywords.

## Handbuch

[OKW4Robot Handbuch](https://hrabovszki1023.github.io/okw-examples/) — Schritt-für-Schritt-Anleitung.

## KI-Testgenerierung

Testfälle können mit jeder KI (Claude, ChatGPT, Copilot, ...) generiert werden.
Die System-Prompts für die Testgenerierung werden zentral in
[`robotframework-okw4robot/prompts/`](https://github.com/Hrabovszki1023/robotframework-okw4robot/tree/main/prompts) gepflegt.

Kopieren Sie den Prompt in Ihre KI und beschreiben Sie in natürlicher Sprache, was Sie testen möchten.
Die KI erzeugt eine lauffähige `.robot`-Datei.

## Lizenz

- **Community** (nicht-kommerziell): siehe [LICENSE](LICENSE)
- **Kommerziell**: Kontakt über den Rechteinhaber

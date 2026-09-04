# OKW Kafka — Quickstart & Configuration

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Robot Framework Test (.robot)                                          │
│                                                                         │
│  KafkaStart ──► YAML Config laden ──► Verbindung zu Broker aufbauen    │
│                                                                         │
│  ┌─── Nachricht aufbauen ──────────────────────────────────────┐       │
│  │  KafkaSetTopic     ──► Ziel-Topic festlegen                 │       │
│  │  KafkaSetValue     ──► JSON-Body aufbauen (Feld für Feld)   │       │
│  │  KafkaSetKey       ──► Partitions-Schlüssel setzen          │       │
│  │  KafkaSetHeader    ──► Kafka-Header setzen                  │       │
│  └─────────────────────────────────────────────────────────────┘       │
│            │                                                            │
│            ▼                                                            │
│  KafkaProduce ──────────────► ┌──────────────────┐                     │
│                                │   Kafka Broker   │                     │
│                                │                  │                     │
│                                │  Topic A  ████   │                     │
│                                │  Topic B  ██     │                     │
│                                │  Topic C  █████  │                     │
│  KafkaConsume ◄───────────── │                  │                     │
│   (Polling mit Timeout)        └──────────────────┘                     │
│            │                                                            │
│            ▼                                                            │
│  ┌─── Empfangene Nachricht prüfen ─────────────────────────────┐       │
│  │  KafkaVerifyValue      ──► JSON-Feld prüfen (EXACT)         │       │
│  │  KafkaVerifyValueWCM   ──► Wildcard-Match (*, ?)            │       │
│  │  KafkaVerifyValueREGX  ──► Regex-Match                      │       │
│  │  KafkaVerifyHeader     ──► Header-Wert prüfen               │       │
│  │  KafkaVerifyKey        ──► Message-Key prüfen               │       │
│  │  KafkaMemorizeValue    ──► Wert merken für $MEM{KEY}        │       │
│  └─────────────────────────────────────────────────────────────┘       │
│                                                                         │
│  KafkaStop ──► Verbindungen schließen                                  │
└─────────────────────────────────────────────────────────────────────────┘
```

### Keyword-Phasen

Das Keyword-Pattern folgt dem universellen OKW-Modell — dieselbe
Abfolge wie bei GUI, REST und SSH:

```
Session ──► Scope ──► Input ──► Action ──► Receive ──► Verify ──► Teardown
  │           │         │         │           │           │          │
  │           │         │         │           │           │          │
KafkaStart  SetTopic  SetValue  Produce   Consume    Verify*     KafkaStop
                      SetKey                          Memorize*
                      SetHeader
```

### Was passiert im Detail?

```
KafkaSetValue  $.action  create        ──► body = {"action": "create"}
KafkaSetValue  $.item    Widget-42     ──► body = {"action": "create", "item": "Widget-42"}
KafkaSetKey    order-001               ──► key  = "order-001"
KafkaSetHeader correlation-id test-1   ──► headers = {"correlation-id": "test-1"}
                                            │
KafkaProduce ◄──────────────────────────────┘
   │  Serialisiert body als JSON
   │  Sendet an Topic mit Key + Headers
   │  Setzt body/key/headers zurück (!)
   ▼
KafkaConsume  order-events  TIMEOUT=30
   │  Pollt Topic bis Nachricht eintrifft
   │  Optional: FILTER=$.item=Widget-42
   │  Speichert Nachricht intern
   ▼
KafkaVerifyValue  $.action  create
   │  Liest Feld aus gespeicherter Nachricht
   │  Vergleicht mit erwartetem Wert
   ▼  PASS oder FAIL
```

**Wichtig:** Nach `KafkaProduce` werden Body, Key und Headers automatisch
zurückgesetzt. Jede neue Nachricht wird von Null aufgebaut.

---

## Quickstart (3 Schritte)

### 1. Installieren

```bash
pip install robotframework-okw-kafka
```

### 2. YAML-Konfiguration erstellen

```yaml
# configs/kafka-local.yaml
bootstrap_servers: "localhost:9092"
consumer_group: "okw-test"
value_serializer: "json"
value_deserializer: "json"
```

Das ist alles, was für den Einstieg nötig ist. Die Datei liegt im
Ordner `configs/` neben den `.robot`-Dateien.

### 3. Ersten Test schreiben

```robot
*** Settings ***
Library    okw_kafka.library.OkwKafkaLibrary
...        config_dir=configs    backend=confluent
Test Setup     KafkaStart    kafka-local
Test Teardown  KafkaStop

*** Test Cases ***
Erste Nachricht Senden Und Empfangen
    KafkaSetTopic       mein-test-topic
    KafkaSetValue       $.greeting    Hallo Welt
    KafkaProduce

    KafkaConsume        mein-test-topic    TIMEOUT=10
    KafkaVerifyValue    $.greeting    Hallo Welt
```

---

## YAML Configuration Reference

### Connection

| Key | Typ | Default | Beschreibung |
|-----|-----|---------|--------------|
| `bootstrap_servers` | String | `"localhost:9092"` | Kafka-Broker Adresse(n). Mehrere mit Komma: `"broker1:9092,broker2:9092"` |
| `consumer_group` | String | `"okw-test"` | Consumer-Group-ID. Jede Testumgebung sollte eine eigene verwenden. |
| `auto_offset_reset` | String | `"latest"` | Wo der Consumer bei einer neuen Group startet: `"earliest"` (alle Nachrichten) oder `"latest"` (nur neue). |

### Serialization

| Key | Typ | Default | Beschreibung |
|-----|-----|---------|--------------|
| `value_serializer` | String | `"json"` | Wie der Nachrichteninhalt beim Senden kodiert wird: `"json"` (→ UTF-8 JSON) oder `"string"` (→ UTF-8 Text). |
| `value_deserializer` | String | `"json"` | Wie der Nachrichteninhalt beim Empfangen dekodiert wird: `"json"` (→ Python dict) oder `"string"` (→ Text). |

### Security (optional)

| Key | Typ | Default | Beschreibung |
|-----|-----|---------|--------------|
| `security_protocol` | String | — | Sicherheitsprotokoll: `"PLAINTEXT"`, `"SSL"`, `"SASL_PLAINTEXT"`, `"SASL_SSL"` |
| `sasl_mechanism` | String | — | SASL-Mechanismus: `"PLAIN"`, `"SCRAM-SHA-256"`, `"SCRAM-SHA-512"` |
| `sasl_username` | String | — | SASL-Benutzername |
| `sasl_password` | String | — | SASL-Passwort |
| `ssl_ca_location` | String | — | Pfad zur CA-Zertifikatsdatei (PEM) |

### Beispiel: Lokaler Entwicklungsserver

```yaml
# Minimale Konfiguration — Broker auf localhost ohne Authentifizierung
bootstrap_servers: "localhost:9092"
consumer_group: "dev-tests"
value_serializer: "json"
value_deserializer: "json"
```

### Beispiel: Testumgebung mit SASL

```yaml
# Broker mit SASL-Authentifizierung im Firmennetz
bootstrap_servers: "kafka-test.firma.local:9092"
consumer_group: "okw-integration-tests"
auto_offset_reset: "earliest"
value_serializer: "json"
value_deserializer: "json"
security_protocol: "SASL_PLAINTEXT"
sasl_mechanism: "SCRAM-SHA-256"
sasl_username: "test-user"
sasl_password: "${KAFKA_PASSWORD}"
```

### Beispiel: Produktion mit SSL

```yaml
# Broker mit TLS-Verschlüsselung und SASL
bootstrap_servers: "kafka-prod-1:9093,kafka-prod-2:9093,kafka-prod-3:9093"
consumer_group: "okw-e2e-tests"
auto_offset_reset: "latest"
value_serializer: "json"
value_deserializer: "json"
security_protocol: "SASL_SSL"
sasl_mechanism: "SCRAM-SHA-512"
sasl_username: "e2e-test-account"
sasl_password: "${KAFKA_PASSWORD}"
ssl_ca_location: "/etc/ssl/certs/kafka-ca.pem"
```

---

## Config-Verzeichnis-Suche

Die Library sucht die YAML-Datei in dieser Reihenfolge:

1. `<Verzeichnis der .robot-Datei>/configs/<name>.yaml`
2. `<Verzeichnis der .robot-Datei>/<name>.yaml`
3. `<Arbeitsverzeichnis>/configs/<name>.yaml`
4. `<Arbeitsverzeichnis>/<name>.yaml`

Die Endung (`.yaml` oder `.yml`) wird automatisch ergänzt.
Pfade mit `..`, `/` oder `\` im Namen werden als Sicherheitsmaßnahme
abgelehnt.

---

## Auto-Typisierung

`KafkaSetValue` konvertiert Werte automatisch:

| Eingabe | Wird zu | Python-Typ |
|---------|---------|------------|
| `42` | `42` | `int` |
| `3.14` | `3.14` | `float` |
| `true` / `false` | `True` / `False` | `bool` |
| `null` | `None` | `NoneType` |
| `Widget-42` | `"Widget-42"` | `str` |

Um die Auto-Konvertierung zu umgehen (z.B. Postleitzahlen als String):

```robot
KafkaSetValueAsString    $.zipcode    01234
```

---

## Filter bei KafkaConsume

`FILTER=$.field=value` filtert Nachrichten beim Konsumieren.
Nur Nachrichten, deren Feld den angegebenen Wert hat, werden akzeptiert:

```robot
# Zwei Nachrichten gesendet, nur die mit item=B wird konsumiert
KafkaConsume    my-topic    FILTER=$.item=B    TIMEOUT=30
```

Nicht-passende Nachrichten werden übersprungen. Wenn innerhalb
des Timeouts keine passende Nachricht eintrifft, schlägt der Test fehl.

---

## Memorize & $MEM{KEY}

Werte aus empfangenen Nachrichten können gespeichert und in
nachfolgenden Keywords wiederverwendet werden:

```robot
# Schritt 1: Wert merken
KafkaConsume             response-topic    TIMEOUT=30
KafkaMemorizeValue       $.orderId    ORDER_ID

# Schritt 2: Gemerkten Wert verwenden
KafkaSetValue            $.ref    $MEM{ORDER_ID}
KafkaProduce
```

Das funktioniert bibliotheksübergreifend — ein in Kafka gelesener
Wert kann auch in SSH- oder REST-Keywords verwendet werden.

---

## Backend: stub vs. confluent

| Backend | Zweck | Broker nötig? |
|---------|-------|---------------|
| `stub` | Contract-Tests, Unit-Tests | Nein — alles in-memory |
| `confluent` | Integration-Tests, E2E | Ja — echter Kafka-Broker |

```robot
# Contract-Test (kein Broker)
Library    okw_kafka.library.OkwKafkaLibrary    backend=stub

# Integration-Test (echter Broker)
Library    okw_kafka.library.OkwKafkaLibrary    config_dir=configs    backend=confluent
```

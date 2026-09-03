# OKW Kafka — Keyword Contract

## Keyword Overview

### Session Lifecycle

| Keyword | Parameter | Description |
|---|---|---|
| `KafkaStart` | `<config>` | Connect via YAML config |
| `KafkaStop` | | Close all producers/consumers |

### Produce

| Keyword | Parameter | Description |
|---|---|---|
| `KafkaSetTopic` | `<topic>` | Set target topic |
| `KafkaSetValue` | `<jsonpath>` `<value>` | Build message body |
| `KafkaSetValueAsString` | `<jsonpath>` `<value>` | Value always as string |
| `KafkaSetHeader` | `<key>` `<value>` | Set Kafka header |
| `KafkaSetKey` | `<key>` | Set message key |
| `KafkaProduce` | `[topic]` | Send message |

### Consume

| Keyword | Parameter | Description |
|---|---|---|
| `KafkaConsume` | `<topic>` `[FILTER]` `[TIMEOUT]` | Poll until matching message or timeout |
| `KafkaConsumeLatest` | `<topic>` `[TIMEOUT]` | Skip backlog, latest only |

### Verify

| Keyword | Parameter | Description |
|---|---|---|
| `KafkaVerifyValue` | `<jsonpath>` `<expected>` | EXACT match |
| `KafkaVerifyValueWCM` | `<jsonpath>` `<pattern>` | Wildcard match |
| `KafkaVerifyValueREGX` | `<jsonpath>` `<regex>` | Regex match |
| `KafkaVerifyHeader` | `<key>` `<expected>` | Verify header value |
| `KafkaVerifyKey` | `<expected>` | Verify message key |

### Memorize

| Keyword | Parameter | Description |
|---|---|---|
| `KafkaMemorizeValue` | `<jsonpath>` `<key>` | Store in $MEM{KEY} |
| `KafkaMemorizeKey` | `<key>` | Store message key |

### Admin

| Keyword | Parameter | Description |
|---|---|---|
| `KafkaVerifyTopicExists` | `<topic>` `<YES/NO>` | Topic exists? |
| `KafkaVerifyPartitionCount` | `<topic>` `<expected>` | Partition count |
| `KafkaLogTopics` | | Log all topics |

## Token Support

All keywords respect OKW global tokens:
- `$IGNORE` — keyword becomes no-op
- `$EMPTY` — explicitly empty value
- `$MEM{KEY}` — value expansion from memorized store

## Match Modes

Verify keywords use okw-contract-utils matchers:
- **EXACT** — exact string equality
- **WCM** — wildcard (`*` = any, `?` = one char)
- **REGX** — Python `re.search`

## Synchronization

`KafkaConsume` uses timeout-based polling (default 30s, configurable).
No `Sleep` in test code — the synchronization is inside the keyword.

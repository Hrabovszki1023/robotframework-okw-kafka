# Changelog

All notable changes to this project will be documented in this file.

## [0.1.1] - 2026-09-10

### Fixed
- Add missing PyYAML dependency to `pyproject.toml`.

## [0.1.0] - 2026-09-03

### Added
- Initial release with 22 keywords for Apache Kafka integration testing.
- Session lifecycle: `KafkaStart`, `KafkaStop`.
- Produce: `KafkaSetTopic`, `KafkaSetValue`, `KafkaSetValueAsString`,
  `KafkaSetHeader`, `KafkaSetKey`, `KafkaProduce`.
- Consume: `KafkaConsume` (with `FILTER` and `TIMEOUT`), `KafkaConsumeLatest`.
- Verify: `KafkaVerifyValue`, `KafkaVerifyValueWCM`, `KafkaVerifyValueREGX`,
  `KafkaVerifyHeader`, `KafkaVerifyKey`.
- Memorize: `KafkaMemorizeValue`, `KafkaMemorizeKey` with `$MEM{KEY}` expansion.
- Admin: `KafkaVerifyTopicExists`, `KafkaVerifyPartitionCount`, `KafkaLogTopics`.
- Full token support: `$IGNORE`, `$EMPTY` on all keywords.
- Stub backend for contract tests without a real broker.
- YAML-based configuration with multi-directory search.
- JSONPath notation for message fields (`$.action`, `$.order.items[0]`).
- Auto-typing: string values converted to int/float/bool/None where applicable.
- 53 contract tests (stub backend).
- 6 integration tests (real Kafka broker in KRaft mode).
- Quickstart documentation with architecture overview.

# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0] - Unreleased

### Highlights
- Initial project structure and keyword stubs
- 22 keywords defined (Session, Produce, Consume, Verify, Memorize, Admin)

### Features
- `OkwKafkaLibrary` with all keyword signatures and libdoc docstrings
- Backend: `confluent-kafka-python`
- YAML-based configuration (bootstrap servers, auth, SSL, consumer group)
- Timeout-based polling in `KafkaConsume` (no Sleep in test code)
- OKW token support ($IGNORE, $EMPTY, WCM, REGX)

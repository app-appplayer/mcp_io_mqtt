## [0.2.1] - 2026-05-23 - mcp_bundle 0.4.0 cascade

### Changed (cascade)
- `mcp_bundle` caret bumped from `^0.3.0` to `^0.4.0`.
- `mcp_io` caret bumped from `^0.2.0` to `^0.2.1`.

mcp_io_mqtt does not touch `UiSection.pages` directly — caret-only cascade. Consumers should bump to `^0.2.1`.

## [0.2.0] - 2026-05-04

- MQTT v5 — properties, reason codes, AUTH packet, shared subscriptions
  (`$share/<group>/<filter>`), client-side topic-alias LRU cache.
- QoS 1 / QoS 2 inflight tracking + persistent session (resume on
  `sessionPresent=true`).
- Will message + retain flag.
- Reason code → `IoError` mapping.

## [0.1.0] - 2026-04-28 - Initial Release

### Added
- MQTT v3.1.1 codec, transport, and adapter for mcp_io.
- QoS 0 pub/sub.
- Topic wildcard matching.

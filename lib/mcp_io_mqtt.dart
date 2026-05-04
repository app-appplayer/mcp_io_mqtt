/// MQTT v3.1.1 + v5 adapter for mcp_io.
library;

// Legacy (BC).
export 'src/mqtt_codec.dart';
export 'src/mqtt_transport.dart';
export 'src/mqtt_adapter.dart';

// Codec primitives.
export 'src/codec/control_packet.dart';
export 'src/codec/properties.dart';
export 'src/codec/reason_codes.dart';
export 'src/codec/variable_byte_integer.dart';
export 'src/codec/topic_alias_cache.dart';

// QoS / inflight tracking + persistent session journal.
export 'src/qos/inflight_tracker.dart';
export 'src/qos/session_journal.dart';

// Subscribe primitives.
export 'src/subscribe/topic_filter.dart';

// Error mapping.
export 'src/mapping/reason_to_ioerror.dart';

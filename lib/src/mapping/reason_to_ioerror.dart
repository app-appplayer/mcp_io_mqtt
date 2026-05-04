import 'package:mcp_bundle/mcp_bundle.dart';

import '../codec/reason_codes.dart';

/// Maps MQTT v5 [MqttReasonCode] to canonical [IoError] codes.
///
class ReasonToIoError {
  ReasonToIoError._();

  /// Convert a reason byte (`MqttReasonCode.value`) to an [IoError].
  /// Pass the raw byte rather than the enum so callers can map even
  /// vendor-extended values consistently.
  static IoError? fromReasonByte(int value, {DateTime? timestamp}) {
    if (value < 0x80) return null;
    final ts = timestamp ?? DateTime.now();
    final code = _ioErrorCode(value);
    return IoError(
      code: code,
      message: 'MQTT reason 0x${value.toRadixString(16).padLeft(2, '0')}',
      timestamp: ts,
    );
  }

  /// True when the reason should trigger transport-level reconnect
  /// (vs a permanent client error).
  static bool requiresReconnect(int value) {
    switch (value) {
      case 0x88: // Server unavailable
      case 0x89: // Server busy (with backoff)
      case 0x8B: // Server shutting down
      case 0x8D: // Keep Alive timeout
      case 0x9C: // Use another server
      case 0x9D: // Server moved
      case 0x9F: // Connection rate exceeded
        return true;
      default:
        return false;
    }
  }

  static String _ioErrorCode(int value) {
    switch (value) {
      case 0x80:
        return 'protocol.unspecified';
      case 0x81:
        return 'protocol.malformed';
      case 0x82:
        return 'protocol.error';
      case 0x83:
        return 'device.error';
      case 0x84:
        return 'protocol.version_unsupported';
      case 0x85:
        return 'auth.client_id_invalid';
      case 0x86:
        return 'auth.bad_credentials';
      case 0x87:
        return 'auth.not_authorized';
      case 0x88:
        return 'device.unavailable';
      case 0x89:
        return 'device.busy';
      case 0x8A:
        return 'auth.banned';
      case 0x8B:
        return 'device.shutdown';
      case 0x8C:
        return 'auth.method_unsupported';
      case 0x8D:
        return 'transport.keepalive';
      case 0x8E:
        return 'auth.session_taken_over';
      case 0x8F:
        return 'protocol.filter_invalid';
      case 0x90:
        return 'protocol.topic_invalid';
      case 0x91:
        return 'protocol.packet_id_in_use';
      case 0x92:
        return 'protocol.packet_id_not_found';
      case 0x93:
        return 'quota.receive_maximum';
      case 0x94:
        return 'protocol.topic_alias_invalid';
      case 0x95:
        return 'protocol.packet_too_large';
      case 0x96:
        return 'quota.rate_exceeded';
      case 0x97:
        return 'quota.exceeded';
      case 0x98:
        return 'device.admin_action';
      case 0x99:
        return 'protocol.payload_format';
      case 0x9A:
        return 'feature.retain_unsupported';
      case 0x9B:
        return 'feature.qos_unsupported';
      case 0x9C:
        return 'device.redirect';
      case 0x9D:
        return 'device.moved';
      case 0x9E:
        return 'feature.shared_sub_unsupported';
      case 0x9F:
        return 'quota.connection_rate';
      case 0xA0:
        return 'quota.max_connect_time';
      case 0xA1:
        return 'feature.sub_id_unsupported';
      case 0xA2:
        return 'feature.wildcard_unsupported';
      default:
        return 'protocol.unknown';
    }
  }
}

/// MQTT v3.1.1 packet codec (pure logic).
///
/// Implements the control packet set needed by a QoS 0 client:
///   CONNECT, CONNACK, PUBLISH, SUBSCRIBE, SUBACK,
///   UNSUBSCRIBE, UNSUBACK, PINGREQ, PINGRESP, DISCONNECT.
///
/// Reference: OASIS MQTT v3.1.1 (ISO/IEC 20922:2016).
library;

import 'dart:convert';
import 'dart:typed_data';

// === Packet types ===

class MqttPacketType {
  static const int connect = 0x10;
  static const int connack = 0x20;
  static const int publish = 0x30;
  static const int puback = 0x40;
  static const int subscribe = 0x80;
  static const int suback = 0x90;
  static const int unsubscribe = 0xA0;
  static const int unsuback = 0xB0;
  static const int pingreq = 0xC0;
  static const int pingresp = 0xD0;
  static const int disconnect = 0xE0;

  const MqttPacketType._();
}

class MqttCodecError implements Exception {
  final String message;
  const MqttCodecError(this.message);
  @override
  String toString() => 'MqttCodecError: $message';
}

// === Remaining-length varint (MQTT v3.1.1 § 2.2.3) ===

/// Encode a non-negative [value] as an MQTT remaining-length varint.
/// Valid range: 0 ..= 268_435_455 (up to 4 bytes).
List<int> encodeRemainingLength(int value) {
  if (value < 0 || value > 0x0FFFFFFF) {
    throw MqttCodecError('remaining length out of range: $value');
  }
  final out = <int>[];
  var v = value;
  do {
    var digit = v & 0x7F;
    v >>= 7;
    if (v > 0) digit |= 0x80;
    out.add(digit);
  } while (v > 0);
  return out;
}

/// Decode a remaining-length varint starting at [offset].
/// Returns (value, bytesRead). Throws on malformed input.
({int value, int bytesRead}) decodeRemainingLength(List<int> bytes, int offset) {
  var multiplier = 1;
  var value = 0;
  var idx = offset;
  int digit;
  do {
    if (idx >= bytes.length) {
      throw const MqttCodecError('remaining length incomplete');
    }
    if (multiplier > 128 * 128 * 128) {
      throw const MqttCodecError('remaining length malformed (too long)');
    }
    digit = bytes[idx];
    value += (digit & 0x7F) * multiplier;
    multiplier *= 128;
    idx++;
  } while ((digit & 0x80) != 0);
  return (value: value, bytesRead: idx - offset);
}

// === UTF-8 string (MQTT § 1.5.3) — 2-byte length prefix ===

List<int> encodeMqttString(String s) {
  final bytes = utf8.encode(s);
  if (bytes.length > 0xFFFF) {
    throw MqttCodecError('MQTT string too long: ${bytes.length}');
  }
  return [
    (bytes.length >> 8) & 0xFF,
    bytes.length & 0xFF,
    ...bytes,
  ];
}

({String value, int bytesRead}) decodeMqttString(List<int> bytes, int offset) {
  if (bytes.length < offset + 2) {
    throw const MqttCodecError('MQTT string truncated (length prefix)');
  }
  final len = (bytes[offset] << 8) | bytes[offset + 1];
  if (bytes.length < offset + 2 + len) {
    throw const MqttCodecError('MQTT string truncated (body)');
  }
  final value = utf8.decode(bytes.sublist(offset + 2, offset + 2 + len));
  return (value: value, bytesRead: 2 + len);
}

// === CONNECT ===

class MqttConnect {
  final String clientId;
  final String? username;
  final String? password;
  final int keepAliveSeconds;
  final bool cleanSession;

  const MqttConnect({
    required this.clientId,
    this.username,
    this.password,
    this.keepAliveSeconds = 60,
    this.cleanSession = true,
  });

  Uint8List encode() {
    // Protocol name "MQTT" + level 4.
    final variableHeader = <int>[
      ...encodeMqttString('MQTT'),
      0x04, // protocol level
      _connectFlags(),
      (keepAliveSeconds >> 8) & 0xFF,
      keepAliveSeconds & 0xFF,
    ];
    final payload = <int>[
      ...encodeMqttString(clientId),
      if (username != null) ...encodeMqttString(username!),
      if (password != null) ...encodeMqttString(password!),
    ];
    final remaining = variableHeader.length + payload.length;
    return Uint8List.fromList([
      MqttPacketType.connect,
      ...encodeRemainingLength(remaining),
      ...variableHeader,
      ...payload,
    ]);
  }

  int _connectFlags() {
    var flags = 0;
    if (cleanSession) flags |= 0x02;
    if (username != null) flags |= 0x80;
    if (password != null) flags |= 0x40;
    return flags;
  }
}

class MqttConnack {
  final bool sessionPresent;
  final int returnCode;   // 0 = accepted

  const MqttConnack({required this.sessionPresent, required this.returnCode});

  /// Parse a CONNACK body (the two bytes after the fixed header).
  factory MqttConnack.fromBody(List<int> body) {
    if (body.length < 2) {
      throw const MqttCodecError('CONNACK body too short');
    }
    return MqttConnack(
      sessionPresent: (body[0] & 0x01) == 0x01,
      returnCode: body[1],
    );
  }
}

// === PUBLISH ===

class MqttPublish {
  final String topic;
  final List<int> payload;
  /// QoS 0 only in this adapter version.
  final int qos;
  final bool retain;
  final bool dup;
  /// Only set for QoS 1/2 (ignored in this iteration).
  final int? packetId;

  const MqttPublish({
    required this.topic,
    required this.payload,
    this.qos = 0,
    this.retain = false,
    this.dup = false,
    this.packetId,
  });

  Uint8List encode() {
    if (qos != 0) {
      throw const MqttCodecError('only QoS 0 is supported in this version');
    }
    final fixedFlags = (dup ? 0x08 : 0) | ((qos & 0x03) << 1) | (retain ? 0x01 : 0);
    final variableHeader = <int>[
      ...encodeMqttString(topic),
      // QoS 0 has no packetId.
    ];
    final remaining = variableHeader.length + payload.length;
    return Uint8List.fromList([
      MqttPacketType.publish | fixedFlags,
      ...encodeRemainingLength(remaining),
      ...variableHeader,
      ...payload,
    ]);
  }

  /// Parse a PUBLISH packet body given the header-flags byte and body bytes.
  factory MqttPublish.fromBody(int headerByte, List<int> body) {
    final dup = (headerByte & 0x08) != 0;
    final qos = (headerByte >> 1) & 0x03;
    final retain = (headerByte & 0x01) != 0;
    final topicResult = decodeMqttString(body, 0);
    var idx = topicResult.bytesRead;
    int? packetId;
    if (qos > 0) {
      if (body.length < idx + 2) {
        throw const MqttCodecError('PUBLISH truncated packet id');
      }
      packetId = (body[idx] << 8) | body[idx + 1];
      idx += 2;
    }
    final payload = body.sublist(idx);
    return MqttPublish(
      topic: topicResult.value,
      payload: List.unmodifiable(payload),
      qos: qos,
      retain: retain,
      dup: dup,
      packetId: packetId,
    );
  }
}

// === SUBSCRIBE / SUBACK ===

class MqttSubscribeEntry {
  final String filter;
  final int qos;
  const MqttSubscribeEntry({required this.filter, this.qos = 0});
}

class MqttSubscribe {
  final int packetId;
  final List<MqttSubscribeEntry> entries;

  const MqttSubscribe({required this.packetId, required this.entries});

  Uint8List encode() {
    final variableHeader = <int>[
      (packetId >> 8) & 0xFF,
      packetId & 0xFF,
    ];
    final payload = <int>[];
    for (final e in entries) {
      payload.addAll(encodeMqttString(e.filter));
      payload.add(e.qos & 0x03);
    }
    final remaining = variableHeader.length + payload.length;
    return Uint8List.fromList([
      // SUBSCRIBE mandates the reserved flag bits = 0010.
      MqttPacketType.subscribe | 0x02,
      ...encodeRemainingLength(remaining),
      ...variableHeader,
      ...payload,
    ]);
  }
}

class MqttSuback {
  final int packetId;
  /// 0/1/2 = success with granted QoS; 0x80 = failure.
  final List<int> returnCodes;

  const MqttSuback({required this.packetId, required this.returnCodes});

  factory MqttSuback.fromBody(List<int> body) {
    if (body.length < 3) {
      throw const MqttCodecError('SUBACK body too short');
    }
    return MqttSuback(
      packetId: (body[0] << 8) | body[1],
      returnCodes: List.unmodifiable(body.sublist(2)),
    );
  }
}

// === UNSUBSCRIBE / UNSUBACK ===

class MqttUnsubscribe {
  final int packetId;
  final List<String> filters;

  const MqttUnsubscribe({required this.packetId, required this.filters});

  Uint8List encode() {
    final variableHeader = <int>[
      (packetId >> 8) & 0xFF,
      packetId & 0xFF,
    ];
    final payload = <int>[];
    for (final f in filters) {
      payload.addAll(encodeMqttString(f));
    }
    final remaining = variableHeader.length + payload.length;
    return Uint8List.fromList([
      MqttPacketType.unsubscribe | 0x02,
      ...encodeRemainingLength(remaining),
      ...variableHeader,
      ...payload,
    ]);
  }
}

// === Fixed-form packets ===

Uint8List encodePingReq() =>
    Uint8List.fromList([MqttPacketType.pingreq, 0x00]);

Uint8List encodeDisconnect() =>
    Uint8List.fromList([MqttPacketType.disconnect, 0x00]);

// === Frame-level parser ===

class MqttIncomingPacket {
  /// The first byte, including the packet type in the upper nibble and flags
  /// in the lower nibble.
  final int headerByte;
  final List<int> body;

  const MqttIncomingPacket({required this.headerByte, required this.body});

  int get packetType => headerByte & 0xF0;
}

/// Attempt to extract a single MQTT packet starting at [offset] in [buffer].
/// Returns the parsed packet and the number of bytes consumed; returns null
/// when the buffer does not yet contain a full packet (caller should wait
/// for more bytes).
({MqttIncomingPacket packet, int bytesRead})? tryParsePacket(
  List<int> buffer, {
  int offset = 0,
}) {
  if (buffer.length <= offset) return null;
  final headerByte = buffer[offset];
  // Decode remaining length.
  final int remainingValue;
  final int lengthBytes;
  try {
    final r = decodeRemainingLength(buffer, offset + 1);
    remainingValue = r.value;
    lengthBytes = r.bytesRead;
  } on MqttCodecError catch (e) {
    if (e.message.contains('incomplete')) return null;
    rethrow;
  }
  final totalLength = 1 + lengthBytes + remainingValue;
  if (buffer.length < offset + totalLength) return null;
  final body = buffer.sublist(
    offset + 1 + lengthBytes,
    offset + totalLength,
  );
  return (
    packet: MqttIncomingPacket(headerByte: headerByte, body: body),
    bytesRead: totalLength,
  );
}

// === Topic matching (§ 4.7) ===

/// Returns `true` when [topic] matches the [filter]. Wildcards:
/// - `+` matches exactly one level
/// - `#` matches zero or more trailing levels (must be the final segment)
bool topicMatches(String filter, String topic) {
  final filterSegs = filter.split('/');
  final topicSegs = topic.split('/');
  for (var i = 0; i < filterSegs.length; i++) {
    final f = filterSegs[i];
    if (f == '#') {
      // Must be the last segment of the filter. Matches any remaining tail
      // (including empty).
      return i == filterSegs.length - 1;
    }
    if (i >= topicSegs.length) return false;
    if (f == '+') continue;
    if (f != topicSegs[i]) return false;
  }
  return filterSegs.length == topicSegs.length;
}

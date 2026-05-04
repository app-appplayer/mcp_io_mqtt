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

import 'codec/properties.dart';

// === Packet types ===

class MqttPacketType {
  static const int connect = 0x10;
  static const int connack = 0x20;
  static const int publish = 0x30;
  static const int puback = 0x40;

  /// QoS 2 — broker → publisher: "I have your message".
  static const int pubrec = 0x50;

  /// QoS 2 — publisher → broker: "release the message".
  static const int pubrel = 0x60;

  /// QoS 2 — broker → publisher: "completed".
  static const int pubcomp = 0x70;

  static const int subscribe = 0x80;
  static const int suback = 0x90;
  static const int unsubscribe = 0xA0;
  static const int unsuback = 0xB0;
  static const int pingreq = 0xC0;
  static const int pingresp = 0xD0;
  static const int disconnect = 0xE0;

  /// MQTT v5 only — enhanced authentication (challenge / response /
  /// re-authenticate). Spec §3.15.
  static const int auth = 0xF0;

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

  /// MQTT protocol level — 4 = v3.1.1 (default), 5 = v5.
  /// When set to 5, [properties] and [willProperties] are emitted
  /// inside the CONNECT packet per Spec §3.1.2.11.
  final int protocolLevel;

  /// CONNECT properties (v5 only). Empty list ↔ a single 0x00 length
  /// byte on the wire. Ignored when [protocolLevel] is 4.
  final List<MqttProperty> properties;

  /// Optional Will (Last Will and Testament) configuration. When
  /// [willTopic] is non-null, the broker publishes [willPayload] to
  /// [willTopic] (with [willQos] / [willRetain]) if the client
  /// disconnects ungracefully.
  final String? willTopic;
  final List<int>? willPayload;
  final int willQos;
  final bool willRetain;

  /// Will properties (v5 only) — emitted between Will Flag and Will
  /// Topic in the payload. Includes `willDelayInterval`,
  /// `payloadFormatIndicator`, `messageExpiryInterval`, `contentType`,
  /// `responseTopic`, `correlationData`, `userProperty`. Ignored
  /// when [protocolLevel] is 4 or [willTopic] is null.
  final List<MqttProperty> willProperties;

  const MqttConnect({
    required this.clientId,
    this.username,
    this.password,
    this.keepAliveSeconds = 60,
    this.cleanSession = true,
    this.willTopic,
    this.willPayload,
    this.willQos = 0,
    this.willRetain = false,
    this.protocolLevel = 4,
    this.properties = const [],
    this.willProperties = const [],
  });

  Uint8List encode() {
    if (protocolLevel != 4 && protocolLevel != 5) {
      throw MqttCodecError('unsupported protocol level: $protocolLevel');
    }
    final variableHeader = <int>[
      ...encodeMqttString('MQTT'),
      protocolLevel,
      _connectFlags(),
      (keepAliveSeconds >> 8) & 0xFF,
      keepAliveSeconds & 0xFF,
      if (protocolLevel == 5) ...encodeProperties(properties),
    ];
    final payload = <int>[
      ...encodeMqttString(clientId),
      if (protocolLevel == 5 && willTopic != null)
        ...encodeProperties(willProperties),
      if (willTopic != null) ...encodeMqttString(willTopic!),
      if (willTopic != null) ...[
        ((willPayload?.length ?? 0) >> 8) & 0xFF,
        (willPayload?.length ?? 0) & 0xFF,
        ...?willPayload,
      ],
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
    if (willTopic != null) {
      flags |= 0x04;                                  // Will Flag
      flags |= (willQos & 0x03) << 3;                 // Will QoS bits 3-4
      if (willRetain) flags |= 0x20;                  // Will Retain bit 5
    }
    if (username != null) flags |= 0x80;
    if (password != null) flags |= 0x40;
    return flags;
  }
}

class MqttConnack {
  final bool sessionPresent;

  /// v3.1.1 return code (0..5) OR v5 reason code (0x00..0xA2 — reason
  /// codes use the same byte slot in the variable header). 0 in both
  /// versions means "accepted".
  final int returnCode;

  /// v5 properties — empty when the response is v3.1.1.
  final List<MqttProperty> properties;

  const MqttConnack({
    required this.sessionPresent,
    required this.returnCode,
    this.properties = const [],
  });

  /// Parse a v3.1.1 CONNACK body (always two bytes).
  factory MqttConnack.fromBody(List<int> body) {
    if (body.length < 2) {
      throw const MqttCodecError('CONNACK body too short');
    }
    return MqttConnack(
      sessionPresent: (body[0] & 0x01) == 0x01,
      returnCode: body[1],
    );
  }

  /// Parse a v5 CONNACK body — `[ack flags, reason code, properties]`.
  /// Spec §3.2.2.
  factory MqttConnack.fromBodyV5(List<int> body) {
    if (body.length < 2) {
      throw const MqttCodecError('CONNACK v5 body too short');
    }
    final r = decodeProperties(body, 2);
    return MqttConnack(
      sessionPresent: (body[0] & 0x01) == 0x01,
      returnCode: body[1],
      properties: r.props,
    );
  }
}

// === PUBLISH ===

class MqttPublish {
  final String topic;
  final List<int> payload;
  final int qos;
  final bool retain;
  final bool dup;

  /// Only set for QoS 1/2.
  final int? packetId;

  /// MQTT protocol level — 4 = v3.1.1 (default, BC), 5 = v5.
  /// When set to 5, [properties] are emitted in the variable header
  /// between packetId and payload.
  final int protocolLevel;

  /// v5 PUBLISH properties (Spec §3.3.2.3). Common entries:
  ///   - `MqttByteProperty(payloadFormatIndicator, 0|1)`
  ///   - `MqttUint32Property(messageExpiryInterval, ...)`
  ///   - `MqttStringProperty(contentType, 'application/json')`
  ///   - `MqttStringProperty(responseTopic, 'reply/...')`
  ///   - `MqttBinaryDataProperty(correlationData, [...])`
  ///   - `MqttUint16Property(topicAlias, n)` (cuts wire bytes for
  ///     repeated topics over the same channel)
  ///   - one or more `MqttUserProperty(...)`
  final List<MqttProperty> properties;

  const MqttPublish({
    required this.topic,
    required this.payload,
    this.qos = 0,
    this.retain = false,
    this.dup = false,
    this.packetId,
    this.protocolLevel = 4,
    this.properties = const [],
  });

  Uint8List encode() {
    if (qos < 0 || qos > 2) {
      throw MqttCodecError('PUBLISH qos out of range: $qos');
    }
    if (qos > 0 && packetId == null) {
      throw const MqttCodecError('PUBLISH with qos>0 requires packetId');
    }
    if (protocolLevel != 4 && protocolLevel != 5) {
      throw MqttCodecError(
        'PUBLISH unsupported protocolLevel: $protocolLevel',
      );
    }
    final fixedFlags =
        (dup ? 0x08 : 0) | ((qos & 0x03) << 1) | (retain ? 0x01 : 0);
    final variableHeader = <int>[
      ...encodeMqttString(topic),
      if (qos > 0) ...[
        (packetId! >> 8) & 0xFF,
        packetId! & 0xFF,
      ],
      if (protocolLevel == 5) ...encodeProperties(properties),
    ];
    final remaining = variableHeader.length + payload.length;
    return Uint8List.fromList([
      MqttPacketType.publish | fixedFlags,
      ...encodeRemainingLength(remaining),
      ...variableHeader,
      ...payload,
    ]);
  }

  /// Parse a v3.1.1 PUBLISH packet body. v5 callers should use
  /// [MqttPublish.fromBodyV5] instead so the property block is
  /// recognised; calling this on a v5 PUBLISH treats the property
  /// block as part of the application payload.
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

  /// Parse a v5 PUBLISH packet body — same as [fromBody] but decodes
  /// the property block that sits between packetId (if any) and
  /// payload.
  factory MqttPublish.fromBodyV5(int headerByte, List<int> body) {
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
    final propRes = decodeProperties(body, idx);
    idx += propRes.length;
    final payload = body.sublist(idx);
    return MqttPublish(
      topic: topicResult.value,
      payload: List.unmodifiable(payload),
      qos: qos,
      retain: retain,
      dup: dup,
      packetId: packetId,
      protocolLevel: 5,
      properties: propRes.props,
    );
  }
}

// === SUBSCRIBE / SUBACK ===

/// Retain-handling option byte (Spec §3.8.3.1).
enum MqttRetainHandling {
  /// Send retained messages at the time of subscription (default).
  sendAtSubscribe(0),

  /// Send retained only if this is a new subscription (no existing
  /// match for the filter).
  sendIfNew(1),

  /// Never send retained.
  doNotSend(2);

  const MqttRetainHandling(this.id);
  final int id;

  static MqttRetainHandling fromId(int id) {
    if (id < 0 || id > 2) {
      throw ArgumentError.value(id, 'id', 'invalid RetainHandling');
    }
    return MqttRetainHandling.values[id];
  }
}

class MqttSubscribeEntry {
  final String filter;
  final int qos;

  /// v5 only: when `true`, server must not echo back to the publisher
  /// even if the subscription matches its own publish.
  final bool noLocal;

  /// v5 only: server preserves the original `retain` flag from the
  /// publisher when delivering retained messages.
  final bool retainAsPublished;

  /// v5 only: how to handle existing retained messages on subscribe.
  final MqttRetainHandling retainHandling;

  const MqttSubscribeEntry({
    required this.filter,
    this.qos = 0,
    this.noLocal = false,
    this.retainAsPublished = false,
    this.retainHandling = MqttRetainHandling.sendAtSubscribe,
  });

  /// Encode the v3.1.1 form — single byte = qos.
  int encodeV3OptionsByte() => qos & 0x03;

  /// Encode the v5 form — qos in bits 0-1, NoLocal in bit 2,
  /// RetainAsPublished in bit 3, RetainHandling in bits 4-5,
  /// reserved (0) in bits 6-7.
  int encodeV5OptionsByte() {
    var b = qos & 0x03;
    if (noLocal) b |= 0x04;
    if (retainAsPublished) b |= 0x08;
    b |= (retainHandling.id & 0x03) << 4;
    return b;
  }

  /// Decode an options byte (works for v3 and v5 — extra bits are
  /// always 0 in v3.1.1).
  factory MqttSubscribeEntry.decodeOptions({
    required String filter,
    required int byte,
  }) {
    return MqttSubscribeEntry(
      filter: filter,
      qos: byte & 0x03,
      noLocal: (byte & 0x04) != 0,
      retainAsPublished: (byte & 0x08) != 0,
      retainHandling: MqttRetainHandling.fromId((byte >> 4) & 0x03),
    );
  }
}

class MqttSubscribe {
  final int packetId;
  final List<MqttSubscribeEntry> entries;

  /// MQTT protocol level — 4 = v3.1.1 (default, BC), 5 = v5.
  final int protocolLevel;

  /// v5 SUBSCRIBE properties (e.g. `subscriptionIdentifier`,
  /// `userProperty`).
  final List<MqttProperty> properties;

  const MqttSubscribe({
    required this.packetId,
    required this.entries,
    this.protocolLevel = 4,
    this.properties = const [],
  });

  Uint8List encode() {
    if (protocolLevel != 4 && protocolLevel != 5) {
      throw MqttCodecError(
        'SUBSCRIBE unsupported protocolLevel: $protocolLevel',
      );
    }
    final variableHeader = <int>[
      (packetId >> 8) & 0xFF,
      packetId & 0xFF,
      if (protocolLevel == 5) ...encodeProperties(properties),
    ];
    final payload = <int>[];
    for (final e in entries) {
      payload.addAll(encodeMqttString(e.filter));
      payload.add(protocolLevel == 5
          ? e.encodeV5OptionsByte()
          : e.encodeV3OptionsByte());
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

  /// v3.1.1: 0/1/2 = success with granted QoS, 0x80 = failure.
  /// v5: full v5 reason code (0x00..0xA1) — typed as `int` to keep
  /// the same field across versions.
  final List<int> returnCodes;

  /// v5 only — properties block.
  final List<MqttProperty> properties;

  const MqttSuback({
    required this.packetId,
    required this.returnCodes,
    this.properties = const [],
  });

  factory MqttSuback.fromBody(List<int> body) {
    if (body.length < 3) {
      throw const MqttCodecError('SUBACK body too short');
    }
    return MqttSuback(
      packetId: (body[0] << 8) | body[1],
      returnCodes: List.unmodifiable(body.sublist(2)),
    );
  }

  /// v5 SUBACK: `[packetId(2), properties block, reason codes...]`.
  factory MqttSuback.fromBodyV5(List<int> body) {
    if (body.length < 3) {
      throw const MqttCodecError('SUBACK v5 body too short');
    }
    final packetId = (body[0] << 8) | body[1];
    final r = decodeProperties(body, 2);
    final codes = body.sublist(2 + r.length);
    return MqttSuback(
      packetId: packetId,
      returnCodes: List.unmodifiable(codes),
      properties: r.props,
    );
  }
}

// === UNSUBSCRIBE / UNSUBACK ===

class MqttUnsubscribe {
  final int packetId;
  final List<String> filters;
  final int protocolLevel;
  final List<MqttProperty> properties;

  const MqttUnsubscribe({
    required this.packetId,
    required this.filters,
    this.protocolLevel = 4,
    this.properties = const [],
  });

  Uint8List encode() {
    if (protocolLevel != 4 && protocolLevel != 5) {
      throw MqttCodecError(
        'UNSUBSCRIBE unsupported protocolLevel: $protocolLevel',
      );
    }
    final variableHeader = <int>[
      (packetId >> 8) & 0xFF,
      packetId & 0xFF,
      if (protocolLevel == 5) ...encodeProperties(properties),
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

class MqttUnsuback {
  final int packetId;

  /// v3.1.1 UNSUBACK has no payload — list is empty. v5 carries one
  /// reason code per filter in the original UNSUBSCRIBE.
  final List<int> reasonCodes;

  /// v5 only.
  final List<MqttProperty> properties;

  const MqttUnsuback({
    required this.packetId,
    this.reasonCodes = const [],
    this.properties = const [],
  });

  factory MqttUnsuback.fromBody(List<int> body) {
    if (body.length < 2) {
      throw const MqttCodecError('UNSUBACK body too short');
    }
    return MqttUnsuback(
      packetId: (body[0] << 8) | body[1],
      reasonCodes: const [],
    );
  }

  factory MqttUnsuback.fromBodyV5(List<int> body) {
    if (body.length < 3) {
      throw const MqttCodecError('UNSUBACK v5 body too short');
    }
    final packetId = (body[0] << 8) | body[1];
    final r = decodeProperties(body, 2);
    final codes = body.sublist(2 + r.length);
    return MqttUnsuback(
      packetId: packetId,
      reasonCodes: List.unmodifiable(codes),
      properties: r.props,
    );
  }
}

// === Fixed-form packets ===

Uint8List encodePingReq() =>
    Uint8List.fromList([MqttPacketType.pingreq, 0x00]);

/// DISCONNECT.
///
/// v3.1.1: empty body. v5: optional `reasonCode` (1 byte) + optional
/// `properties` block. The encoder emits the *short* v3 form when
/// `protocolLevel = 4` (BC) or when `protocolLevel = 5` AND
/// `reasonCode == 0` AND `properties` is empty — matching the
/// observed behaviour of every common broker.
Uint8List encodeDisconnect({
  int protocolLevel = 4,
  int reasonCode = 0,
  List<MqttProperty> properties = const [],
}) {
  if (protocolLevel != 4 && protocolLevel != 5) {
    throw MqttCodecError('DISCONNECT unsupported protocolLevel: $protocolLevel');
  }
  // Short form when nothing to add.
  if (protocolLevel == 4 || (reasonCode == 0 && properties.isEmpty)) {
    return Uint8List.fromList([MqttPacketType.disconnect, 0x00]);
  }
  final body = <int>[
    reasonCode & 0xFF,
    ...encodeProperties(properties),
  ];
  return Uint8List.fromList([
    MqttPacketType.disconnect,
    ...encodeRemainingLength(body.length),
    ...body,
  ]);
}

// === QoS 1 / 2 acks (PUBACK / PUBREC / PUBREL / PUBCOMP) ===

/// PUBACK — QoS 1 ack.
///
/// v3.1.1: `[type, 0x02, idHi, idLo]`. v5: optional reason code + optional
/// properties — when `reasonCode == 0` AND `properties` is empty the
/// short v3-form is emitted (most brokers prefer this for brevity).
Uint8List encodePuback(
  int packetId, {
  int protocolLevel = 4,
  int reasonCode = 0,
  List<MqttProperty> properties = const [],
}) =>
    _encodeQosAck(MqttPacketType.puback, packetId,
        protocolLevel: protocolLevel,
        reasonCode: reasonCode,
        properties: properties);

/// PUBREC — server → publisher, intermediate step of the QoS 2
/// 4-way handshake.
Uint8List encodePubrec(
  int packetId, {
  int protocolLevel = 4,
  int reasonCode = 0,
  List<MqttProperty> properties = const [],
}) =>
    _encodeQosAck(MqttPacketType.pubrec, packetId,
        protocolLevel: protocolLevel,
        reasonCode: reasonCode,
        properties: properties);

/// PUBREL — publisher → server (or server → subscriber for inbound
/// QoS 2). Fixed-header low nibble must be 0x02 per spec.
Uint8List encodePubrel(
  int packetId, {
  int protocolLevel = 4,
  int reasonCode = 0,
  List<MqttProperty> properties = const [],
}) {
  if (protocolLevel != 4 && protocolLevel != 5) {
    throw MqttCodecError('PUBREL unsupported protocolLevel: $protocolLevel');
  }
  if (protocolLevel == 4 || (reasonCode == 0 && properties.isEmpty)) {
    return Uint8List.fromList([
      MqttPacketType.pubrel | 0x02,
      0x02,
      (packetId >> 8) & 0xFF,
      packetId & 0xFF,
    ]);
  }
  final body = <int>[
    (packetId >> 8) & 0xFF,
    packetId & 0xFF,
    reasonCode & 0xFF,
    ...encodeProperties(properties),
  ];
  return Uint8List.fromList([
    MqttPacketType.pubrel | 0x02,
    ...encodeRemainingLength(body.length),
    ...body,
  ]);
}

/// PUBCOMP — final ack of the QoS 2 4-way handshake.
Uint8List encodePubcomp(
  int packetId, {
  int protocolLevel = 4,
  int reasonCode = 0,
  List<MqttProperty> properties = const [],
}) =>
    _encodeQosAck(MqttPacketType.pubcomp, packetId,
        protocolLevel: protocolLevel,
        reasonCode: reasonCode,
        properties: properties);

/// Decode the 2-byte packet id from a PUBACK / PUBREC / PUBREL /
/// PUBCOMP body. Returns -1 when the body is truncated. Works for
/// both v3 and v5 wire forms (the packet id always sits at offset 0).
int decodeAckPacketId(List<int> body) {
  if (body.length < 2) return -1;
  return (body[0] << 8) | body[1];
}

Uint8List _encodeQosAck(
  int type,
  int packetId, {
  required int protocolLevel,
  required int reasonCode,
  required List<MqttProperty> properties,
}) {
  if (protocolLevel != 4 && protocolLevel != 5) {
    throw MqttCodecError('QoS ack unsupported protocolLevel: $protocolLevel');
  }
  if (protocolLevel == 4 || (reasonCode == 0 && properties.isEmpty)) {
    return Uint8List.fromList([
      type,
      0x02,
      (packetId >> 8) & 0xFF,
      packetId & 0xFF,
    ]);
  }
  final body = <int>[
    (packetId >> 8) & 0xFF,
    packetId & 0xFF,
    reasonCode & 0xFF,
    ...encodeProperties(properties),
  ];
  return Uint8List.fromList([
    type,
    ...encodeRemainingLength(body.length),
    ...body,
  ]);
}

/// Decoded PUBACK / PUBREC / PUBREL / PUBCOMP. v3.1.1 carries only
/// `packetId`; v5 adds optional `reasonCode` (default 0 = success) +
/// optional `properties`.
class MqttQosAck {
  final int packetId;
  final int reasonCode;
  final List<MqttProperty> properties;

  const MqttQosAck({
    required this.packetId,
    this.reasonCode = 0,
    this.properties = const [],
  });

  /// Decode a v3.1.1 ack body (always two bytes — packetId only).
  factory MqttQosAck.fromBody(List<int> body) {
    if (body.length < 2) {
      throw const MqttCodecError('QoS ack body too short');
    }
    return MqttQosAck(packetId: (body[0] << 8) | body[1]);
  }

  /// Decode a v5 ack body. Layout: `[packetId(2), reasonCode(1)?, properties?]`.
  /// When the body is exactly 2 bytes, treat as the short form
  /// (reasonCode 0, no properties).
  factory MqttQosAck.fromBodyV5(List<int> body) {
    if (body.length < 2) {
      throw const MqttCodecError('QoS ack v5 body too short');
    }
    final packetId = (body[0] << 8) | body[1];
    if (body.length == 2) {
      return MqttQosAck(packetId: packetId);
    }
    if (body.length == 3) {
      return MqttQosAck(packetId: packetId, reasonCode: body[2]);
    }
    final r = decodeProperties(body, 3);
    return MqttQosAck(
      packetId: packetId,
      reasonCode: body[2],
      properties: r.props,
    );
  }
}

/// AUTH — MQTT v5 enhanced authentication packet (spec §3.15).
///
/// Used for SCRAM, OAuth-like challenge/response, and re-authentication
/// flows during the SASL-style handshake. Only valid when the broker
/// signalled an authentication method during CONNECT/CONNACK.
///
/// Layout (v5 only — v3.1.1 has no AUTH packet):
/// `[reasonCode(1), properties]`. When `reasonCode == 0` AND
/// `properties` is empty, the body is **omitted entirely** per spec
/// §3.15.2.1 ("If the Reason Code is 0x00 and there are no Properties,
/// then the value 0x00 MUST be used").
class MqttAuth {
  /// One of the AUTH reason codes:
  /// - `0x00` Success
  /// - `0x18` Continue authentication
  /// - `0x19` Re-authenticate
  /// (other values surfaced from the broker via [reasonCode]).
  final int reasonCode;

  /// Properties carrying `authenticationMethod` (0x15),
  /// `authenticationData` (0x16), reasonString, userProperties.
  final List<MqttProperty> properties;

  const MqttAuth({
    this.reasonCode = 0,
    this.properties = const [],
  });

  /// Decode an AUTH body. The fixed-header is consumed by the caller.
  factory MqttAuth.fromBody(List<int> body) {
    if (body.isEmpty) return const MqttAuth();
    if (body.length == 1) return MqttAuth(reasonCode: body[0]);
    final r = decodeProperties(body, 1);
    return MqttAuth(reasonCode: body[0], properties: r.props);
  }
}

/// AUTH reason codes (MQTT v5 §3.15.2.1 + Table 2-12).
class MqttAuthReason {
  /// `0x00` Success — final response of the handshake.
  static const int success = 0x00;

  /// `0x18` Continue authentication — challenge or intermediate step.
  static const int continueAuthentication = 0x18;

  /// `0x19` Re-authenticate — initiate re-authentication on an
  /// established connection (client-initiated).
  static const int reAuthenticate = 0x19;

  MqttAuthReason._();
}

/// Encode an AUTH packet (v5 only). Throws when [protocolLevel] is not
/// 5 because AUTH does not exist in v3.1.1.
Uint8List encodeAuth({
  int protocolLevel = 5,
  int reasonCode = 0,
  List<MqttProperty> properties = const [],
}) {
  if (protocolLevel != 5) {
    throw MqttCodecError('AUTH requires protocolLevel 5; got $protocolLevel');
  }
  if (reasonCode == 0 && properties.isEmpty) {
    return Uint8List.fromList([MqttPacketType.auth, 0x00]);
  }
  final body = <int>[
    reasonCode & 0xFF,
    ...encodeProperties(properties),
  ];
  return Uint8List.fromList([
    MqttPacketType.auth,
    ...encodeRemainingLength(body.length),
    ...body,
  ]);
}

/// Decoded DISCONNECT — v3.1.1 has no body, v5 may carry a reason
/// code + properties.
class MqttDisconnect {
  final int reasonCode;
  final List<MqttProperty> properties;

  const MqttDisconnect({
    this.reasonCode = 0,
    this.properties = const [],
  });

  factory MqttDisconnect.fromBody(List<int> body) {
    return const MqttDisconnect();
  }

  factory MqttDisconnect.fromBodyV5(List<int> body) {
    if (body.isEmpty) return const MqttDisconnect();
    if (body.length == 1) return MqttDisconnect(reasonCode: body[0]);
    final r = decodeProperties(body, 1);
    return MqttDisconnect(
      reasonCode: body[0],
      properties: r.props,
    );
  }
}

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

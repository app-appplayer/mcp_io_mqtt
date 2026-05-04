/// MQTT v5 property identifiers.
///
/// Spec ref: MQTT v5 §2.2.2.2.
enum MqttPropertyId {
  payloadFormatIndicator(0x01),
  messageExpiryInterval(0x02),
  contentType(0x03),
  responseTopic(0x08),
  correlationData(0x09),
  subscriptionIdentifier(0x0B),
  sessionExpiryInterval(0x11),
  assignedClientIdentifier(0x12),
  serverKeepAlive(0x13),
  authenticationMethod(0x15),
  authenticationData(0x16),
  requestProblemInformation(0x17),
  willDelayInterval(0x18),
  requestResponseInformation(0x19),
  responseInformation(0x1A),
  serverReference(0x1C),
  reasonString(0x1F),
  receiveMaximum(0x21),
  topicAliasMaximum(0x22),
  topicAlias(0x23),
  maximumQoS(0x24),
  retainAvailable(0x25),
  userProperty(0x26),
  maximumPacketSize(0x27),
  wildcardSubscriptionAvailable(0x28),
  subscriptionIdentifierAvailable(0x29),
  sharedSubscriptionAvailable(0x2A);

  const MqttPropertyId(this.id);
  final int id;

  static MqttPropertyId fromId(int id) {
    return MqttPropertyId.values
        .firstWhere((p) => p.id == id,
            orElse: () => throw FormatException(
                'unknown MQTT property id: 0x${id.toRadixString(16)}'));
  }
}

/// A single v5 property as a tagged value. Only the dart types
/// actually used by the wire protocol are exposed; higher layers wrap
/// these into typed accessors.
sealed class MqttProperty {
  MqttPropertyId get id;
}

class MqttByteProperty extends MqttProperty {
  MqttByteProperty(this.id, this.value);
  @override
  final MqttPropertyId id;
  final int value;
}

class MqttUint16Property extends MqttProperty {
  MqttUint16Property(this.id, this.value);
  @override
  final MqttPropertyId id;
  final int value;
}

class MqttUint32Property extends MqttProperty {
  MqttUint32Property(this.id, this.value);
  @override
  final MqttPropertyId id;
  final int value;
}

class MqttVariableIntProperty extends MqttProperty {
  MqttVariableIntProperty(this.id, this.value);
  @override
  final MqttPropertyId id;
  final int value;
}

class MqttStringProperty extends MqttProperty {
  MqttStringProperty(this.id, this.value);
  @override
  final MqttPropertyId id;
  final String value;
}

class MqttBinaryDataProperty extends MqttProperty {
  MqttBinaryDataProperty(this.id, this.value);
  @override
  final MqttPropertyId id;
  final List<int> value;
}

/// Repeated `User Property` (key/value pair, 0x26). Multiple instances
/// allowed in a single property block.
class MqttUserProperty extends MqttProperty {
  MqttUserProperty(this.key, this.value);
  @override
  MqttPropertyId get id => MqttPropertyId.userProperty;
  final String key;
  final String value;
}

/// Wire encoding kind for each property id (Spec §2.2.2.2).
enum _PropertyWireType {
  byteOne, // 1-byte uint
  uint16,
  uint32,
  variableInt,
  utf8String,
  binaryData,
  utf8StringPair, // user property only
}

const Map<MqttPropertyId, _PropertyWireType> _propertyTypes = {
  MqttPropertyId.payloadFormatIndicator: _PropertyWireType.byteOne,
  MqttPropertyId.messageExpiryInterval: _PropertyWireType.uint32,
  MqttPropertyId.contentType: _PropertyWireType.utf8String,
  MqttPropertyId.responseTopic: _PropertyWireType.utf8String,
  MqttPropertyId.correlationData: _PropertyWireType.binaryData,
  MqttPropertyId.subscriptionIdentifier: _PropertyWireType.variableInt,
  MqttPropertyId.sessionExpiryInterval: _PropertyWireType.uint32,
  MqttPropertyId.assignedClientIdentifier: _PropertyWireType.utf8String,
  MqttPropertyId.serverKeepAlive: _PropertyWireType.uint16,
  MqttPropertyId.authenticationMethod: _PropertyWireType.utf8String,
  MqttPropertyId.authenticationData: _PropertyWireType.binaryData,
  MqttPropertyId.requestProblemInformation: _PropertyWireType.byteOne,
  MqttPropertyId.willDelayInterval: _PropertyWireType.uint32,
  MqttPropertyId.requestResponseInformation: _PropertyWireType.byteOne,
  MqttPropertyId.responseInformation: _PropertyWireType.utf8String,
  MqttPropertyId.serverReference: _PropertyWireType.utf8String,
  MqttPropertyId.reasonString: _PropertyWireType.utf8String,
  MqttPropertyId.receiveMaximum: _PropertyWireType.uint16,
  MqttPropertyId.topicAliasMaximum: _PropertyWireType.uint16,
  MqttPropertyId.topicAlias: _PropertyWireType.uint16,
  MqttPropertyId.maximumQoS: _PropertyWireType.byteOne,
  MqttPropertyId.retainAvailable: _PropertyWireType.byteOne,
  MqttPropertyId.userProperty: _PropertyWireType.utf8StringPair,
  MqttPropertyId.maximumPacketSize: _PropertyWireType.uint32,
  MqttPropertyId.wildcardSubscriptionAvailable: _PropertyWireType.byteOne,
  MqttPropertyId.subscriptionIdentifierAvailable: _PropertyWireType.byteOne,
  MqttPropertyId.sharedSubscriptionAvailable: _PropertyWireType.byteOne,
};

/// Encode a list of [props] into a v5 property block.
///
/// Wire layout: `[VBI total length][id(VBI) + value]*`. The
/// total-length prefix counts only the entry bytes (excluding itself).
List<int> encodeProperties(List<MqttProperty> props) {
  final entries = <int>[];
  for (final p in props) {
    final wire = _propertyTypes[p.id];
    if (wire == null) {
      throw ArgumentError('unknown MQTT property id: ${p.id}');
    }
    entries.addAll(_encodeId(p.id.id));
    entries.addAll(_encodeOne(p, wire));
  }
  return [
    ..._encodeId(entries.length),
    ...entries,
  ];
}

/// Decode a v5 property block starting at [offset]. Returns the parsed
/// list and the total byte count consumed (including the leading VBI
/// length prefix).
({List<MqttProperty> props, int length}) decodeProperties(
    List<int> bytes, int offset) {
  final hdr = _decodeVbi(bytes, offset);
  final blockLen = hdr.value;
  final headerLen = hdr.length;
  final start = offset + headerLen;
  final end = start + blockLen;
  if (end > bytes.length) {
    throw const FormatException('properties: block extends past buffer');
  }
  final props = <MqttProperty>[];
  var cursor = start;
  while (cursor < end) {
    final idVbi = _decodeVbi(bytes, cursor);
    cursor += idVbi.length;
    final propId = MqttPropertyId.fromId(idVbi.value);
    final wire = _propertyTypes[propId]!;
    final r = _decodeOne(bytes, cursor, propId, wire);
    cursor += r.length;
    props.add(r.prop);
  }
  if (cursor != end) {
    throw const FormatException(
        'properties: declared length did not match consumed bytes');
  }
  return (props: props, length: headerLen + blockLen);
}

// === Internal ===

List<int> _encodeId(int v) {
  // Property ids fit in 1-2 bytes today, but spec says VBI — encode
  // through the same VBI helper as remaining-length.
  final out = <int>[];
  var x = v;
  do {
    var b = x & 0x7F;
    x >>= 7;
    if (x > 0) b |= 0x80;
    out.add(b);
  } while (x > 0);
  return out;
}

({int value, int length}) _decodeVbi(List<int> bytes, int offset) {
  var multiplier = 1;
  var value = 0;
  var consumed = 0;
  while (true) {
    if (offset + consumed >= bytes.length) {
      throw const FormatException('properties: VBI truncated');
    }
    final byte = bytes[offset + consumed];
    consumed++;
    value += (byte & 0x7F) * multiplier;
    if (multiplier > 128 * 128 * 128) {
      throw const FormatException('properties: VBI overflow');
    }
    if ((byte & 0x80) == 0) break;
    multiplier *= 128;
  }
  return (value: value, length: consumed);
}

List<int> _encodeOne(MqttProperty p, _PropertyWireType wire) {
  switch (wire) {
    case _PropertyWireType.byteOne:
      return [(p as MqttByteProperty).value & 0xFF];
    case _PropertyWireType.uint16:
      final v = (p as MqttUint16Property).value & 0xFFFF;
      return [(v >> 8) & 0xFF, v & 0xFF];
    case _PropertyWireType.uint32:
      final v = (p as MqttUint32Property).value & 0xFFFFFFFF;
      return [
        (v >> 24) & 0xFF,
        (v >> 16) & 0xFF,
        (v >> 8) & 0xFF,
        v & 0xFF,
      ];
    case _PropertyWireType.variableInt:
      return _encodeId((p as MqttVariableIntProperty).value);
    case _PropertyWireType.utf8String:
      final s = (p as MqttStringProperty).value;
      final bytes = _utf8(s);
      return [(bytes.length >> 8) & 0xFF, bytes.length & 0xFF, ...bytes];
    case _PropertyWireType.binaryData:
      final data = (p as MqttBinaryDataProperty).value;
      return [(data.length >> 8) & 0xFF, data.length & 0xFF, ...data];
    case _PropertyWireType.utf8StringPair:
      final up = p as MqttUserProperty;
      final k = _utf8(up.key);
      final v = _utf8(up.value);
      return [
        (k.length >> 8) & 0xFF, k.length & 0xFF, ...k,
        (v.length >> 8) & 0xFF, v.length & 0xFF, ...v,
      ];
  }
}

({MqttProperty prop, int length}) _decodeOne(
    List<int> bytes, int offset, MqttPropertyId id, _PropertyWireType wire) {
  switch (wire) {
    case _PropertyWireType.byteOne:
      _need(bytes, offset, 1);
      return (
        prop: MqttByteProperty(id, bytes[offset]),
        length: 1,
      );
    case _PropertyWireType.uint16:
      _need(bytes, offset, 2);
      return (
        prop: MqttUint16Property(id, (bytes[offset] << 8) | bytes[offset + 1]),
        length: 2,
      );
    case _PropertyWireType.uint32:
      _need(bytes, offset, 4);
      final v = (bytes[offset] << 24) |
          (bytes[offset + 1] << 16) |
          (bytes[offset + 2] << 8) |
          bytes[offset + 3];
      return (prop: MqttUint32Property(id, v), length: 4);
    case _PropertyWireType.variableInt:
      final r = _decodeVbi(bytes, offset);
      return (
        prop: MqttVariableIntProperty(id, r.value),
        length: r.length,
      );
    case _PropertyWireType.utf8String:
      final r = _readUtf8(bytes, offset);
      return (prop: MqttStringProperty(id, r.value), length: r.length);
    case _PropertyWireType.binaryData:
      _need(bytes, offset, 2);
      final len = (bytes[offset] << 8) | bytes[offset + 1];
      _need(bytes, offset + 2, len);
      return (
        prop: MqttBinaryDataProperty(
          id, bytes.sublist(offset + 2, offset + 2 + len),
        ),
        length: 2 + len,
      );
    case _PropertyWireType.utf8StringPair:
      final k = _readUtf8(bytes, offset);
      final v = _readUtf8(bytes, offset + k.length);
      return (
        prop: MqttUserProperty(k.value, v.value),
        length: k.length + v.length,
      );
  }
}

({String value, int length}) _readUtf8(List<int> bytes, int offset) {
  _need(bytes, offset, 2);
  final len = (bytes[offset] << 8) | bytes[offset + 1];
  _need(bytes, offset + 2, len);
  final raw = bytes.sublist(offset + 2, offset + 2 + len);
  return (value: String.fromCharCodes(raw), length: 2 + len);
}

List<int> _utf8(String s) => s.codeUnits;

void _need(List<int> bytes, int offset, int n) {
  if (offset + n > bytes.length) {
    throw const FormatException('properties: truncated value');
  }
}

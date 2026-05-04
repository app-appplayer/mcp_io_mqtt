/// MQTT control packet types (high nibble of fixed header byte 0).
///
/// Spec ref: MQTT v3.1.1 §2.2.1 / v5 §2.1.2.
enum MqttControlPacketType {
  reserved(0),
  connect(1),
  connAck(2),
  publish(3),
  pubAck(4),
  pubRec(5),
  pubRel(6),
  pubComp(7),
  subscribe(8),
  subAck(9),
  unsubscribe(10),
  unsubAck(11),
  pingReq(12),
  pingResp(13),
  disconnect(14),
  auth(15);

  const MqttControlPacketType(this.id);
  final int id;

  static MqttControlPacketType fromHighNibble(int header) {
    final id = (header >> 4) & 0x0F;
    if (id < 0 || id > 15) {
      throw ArgumentError.value(id, 'id', 'invalid packet type id');
    }
    return MqttControlPacketType.values[id];
  }

  /// Reserved / mandatory low-nibble flags per spec §2.1.3.
  ///
  /// PUBREL, SUBSCRIBE, UNSUBSCRIBE require flags = `0010` per spec.
  /// Other packets use `0000` reserved for future use, except PUBLISH
  /// which encodes DUP/QoS/RETAIN dynamically.
  int get fixedFlags {
    switch (this) {
      case MqttControlPacketType.pubRel:
      case MqttControlPacketType.subscribe:
      case MqttControlPacketType.unsubscribe:
        return 0x02;
      case MqttControlPacketType.publish:
        return 0; // dynamic — caller composes DUP/QoS/RETAIN
      default:
        return 0;
    }
  }
}

/// QoS level (PUBLISH header bits 1-2).
enum MqttQos {
  atMostOnce(0),
  atLeastOnce(1),
  exactlyOnce(2);

  const MqttQos(this.id);
  final int id;

  static MqttQos fromId(int id) {
    if (id < 0 || id > 2) {
      throw ArgumentError.value(id, 'qos', 'must be 0/1/2');
    }
    return MqttQos.values[id];
  }
}

/// PUBLISH fixed header flags (DUP / QoS / RETAIN).
class PublishFlags {
  const PublishFlags({
    this.dup = false,
    this.qos = MqttQos.atMostOnce,
    this.retain = false,
  });

  final bool dup;
  final MqttQos qos;
  final bool retain;

  /// Encode the 4 low-bit nibble of the fixed header byte.
  int toFlagsByte() {
    var b = qos.id << 1;
    if (dup) b |= 0x08;
    if (retain) b |= 0x01;
    return b & 0x0F;
  }

  factory PublishFlags.fromFlagsByte(int b) {
    return PublishFlags(
      dup: (b & 0x08) != 0,
      qos: MqttQos.fromId((b >> 1) & 0x03),
      retain: (b & 0x01) != 0,
    );
  }
}

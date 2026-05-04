/// MQTT v5 PUBACK / PUBREC / PUBREL / PUBCOMP / DISCONNECT
/// reason-code + property block tests.
library;

import 'package:mcp_io_mqtt/mcp_io_mqtt.dart';
import 'package:test/test.dart';

void main() {
  group('Short-form (BC) — v3.1.1 default behaviour', () {
    test('TC-ACK5-001 v3 PUBACK is the same 4 bytes as before', () {
      final bytes = encodePuback(7);
      expect(bytes, [MqttPacketType.puback, 0x02, 0x00, 0x07]);
    });

    test('TC-ACK5-002 v3 PUBREL keeps the mandatory 0x02 low nibble', () {
      final bytes = encodePubrel(9);
      expect(bytes, [MqttPacketType.pubrel | 0x02, 0x02, 0x00, 0x09]);
    });

    test('TC-ACK5-003 v3 DISCONNECT has empty body', () {
      final bytes = encodeDisconnect();
      expect(bytes, [MqttPacketType.disconnect, 0x00]);
    });

    test('TC-ACK5-004 v5 with reason=0 + empty props falls back to short '
        'form', () {
      final bytes = encodePuback(3, protocolLevel: 5);
      expect(bytes, [MqttPacketType.puback, 0x02, 0x00, 0x03]);
    });
  });

  group('v5 long-form encoders', () {
    test('TC-ACK5-005 PUBACK with non-zero reason emits packetId + reason '
        '(no props block when properties empty)', () {
      // Spec §3.4.2.2 — when only reason code is present, the props
      // section may be omitted. Our encoder still emits the empty
      // block (1 byte) for consistency, so total body = 4 bytes.
      final bytes = encodePuback(
        7, protocolLevel: 5, reasonCode: 0x10, // NoMatchingSubscribers
      );
      // Layout: [type, remLen, idHi, idLo, reasonCode, propLen=0].
      expect(bytes[0], MqttPacketType.puback);
      expect(bytes[1], 0x04);                    // remaining length
      expect(bytes[2], 0x00);
      expect(bytes[3], 0x07);
      expect(bytes[4], 0x10);
      expect(bytes[5], 0x00);                    // empty properties
    });

    test('TC-ACK5-006 PUBACK with reason + reasonString property', () {
      final bytes = encodePuback(
        7, protocolLevel: 5, reasonCode: 0x91,
        properties: [
          MqttStringProperty(MqttPropertyId.reasonString, 'PacketIdentifierInUse'),
        ],
      );
      // Body: [idHi, idLo, reason, propLen, propId, len2, ...]
      // Decode and verify the wire roundtrip.
      // Strip the 2-byte fixed header (type + remaining-length VBI).
      final remaining = bytes[1];
      final body = bytes.sublist(2, 2 + remaining);
      final ack = MqttQosAck.fromBodyV5(body);
      expect(ack.packetId, 7);
      expect(ack.reasonCode, 0x91);
      expect(ack.properties, hasLength(1));
      expect(
        (ack.properties.first as MqttStringProperty).value,
        'PacketIdentifierInUse',
      );
    });

    test('TC-ACK5-007 PUBREL v5 long form sets the 0x02 fixed-header bits',
        () {
      final bytes = encodePubrel(
        5, protocolLevel: 5, reasonCode: 0x92,
      );
      expect(bytes[0], MqttPacketType.pubrel | 0x02);
      expect(bytes[1], 0x04); // remaining length
      expect(bytes[2], 0x00);
      expect(bytes[3], 0x05);
      expect(bytes[4], 0x92); // PacketIdentifierNotFound
    });

    test('TC-ACK5-008 DISCONNECT v5 with reason + property block', () {
      final bytes = encodeDisconnect(
        protocolLevel: 5,
        reasonCode: 0x8E, // SessionTakenOver
        properties: [
          MqttStringProperty(
              MqttPropertyId.reasonString, 'session reused'),
        ],
      );
      expect(bytes[0], MqttPacketType.disconnect);
      // remaining length VBI byte at index 1.
      final remaining = bytes[1];
      final body = bytes.sublist(2, 2 + remaining);
      final dec = MqttDisconnect.fromBodyV5(body);
      expect(dec.reasonCode, 0x8E);
      expect((dec.properties.first as MqttStringProperty).value,
          'session reused');
    });
  });

  group('MqttQosAck.fromBody / fromBodyV5', () {
    test('TC-ACK5-009 v3 fromBody reads only packetId', () {
      final ack = MqttQosAck.fromBody(const [0x00, 0x10]);
      expect(ack.packetId, 0x10);
      expect(ack.reasonCode, 0);
      expect(ack.properties, isEmpty);
    });

    test('TC-ACK5-010 v5 fromBodyV5 — short form (2 bytes)', () {
      final ack = MqttQosAck.fromBodyV5(const [0x00, 0x10]);
      expect(ack.packetId, 0x10);
      expect(ack.reasonCode, 0);
    });

    test('TC-ACK5-011 v5 fromBodyV5 — packetId + reason only (3 bytes)',
        () {
      final ack = MqttQosAck.fromBodyV5(const [0x00, 0x10, 0x91]);
      expect(ack.reasonCode, 0x91);
      expect(ack.properties, isEmpty);
    });

    test('TC-ACK5-012 v5 fromBodyV5 — packetId + reason + props', () {
      final propBlock = encodeProperties([
        MqttUserProperty('app', 'mcp_io'),
      ]);
      final body = [0x00, 0x10, 0x10, ...propBlock];
      final ack = MqttQosAck.fromBodyV5(body);
      expect(ack.reasonCode, 0x10);
      expect(ack.properties, hasLength(1));
    });
  });

  group('MqttDisconnect.fromBodyV5', () {
    test('TC-ACK5-013 empty body decodes to default', () {
      final dec = MqttDisconnect.fromBodyV5(const []);
      expect(dec.reasonCode, 0);
      expect(dec.properties, isEmpty);
    });

    test('TC-ACK5-014 single-byte body is reason code only', () {
      final dec = MqttDisconnect.fromBodyV5(const [0x8E]);
      expect(dec.reasonCode, 0x8E);
      expect(dec.properties, isEmpty);
    });

    test('TC-ACK5-015 multi-byte body parses reason + properties', () {
      final propBlock = encodeProperties([
        MqttStringProperty(MqttPropertyId.reasonString, 'goodbye'),
      ]);
      final body = [0x8E, ...propBlock];
      final dec = MqttDisconnect.fromBodyV5(body);
      expect(dec.reasonCode, 0x8E);
      expect((dec.properties.first as MqttStringProperty).value, 'goodbye');
    });
  });

  group('Validation', () {
    test('TC-ACK5-016 unsupported protocolLevel throws on every encoder',
        () {
      expect(
        () => encodePuback(1, protocolLevel: 7),
        throwsA(isA<MqttCodecError>()),
      );
      expect(
        () => encodePubrel(1, protocolLevel: 7),
        throwsA(isA<MqttCodecError>()),
      );
      expect(
        () => encodeDisconnect(protocolLevel: 7),
        throwsA(isA<MqttCodecError>()),
      );
    });
  });
}

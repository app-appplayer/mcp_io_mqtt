import 'package:mcp_io_mqtt/mcp_io_mqtt.dart';
import 'package:test/test.dart';

void main() {
  group('MqttConnect v5 mode', () {
    test('TC-CON5-001 protocolLevel 5 emits property block (empty default)',
        () {
      final bytes = const MqttConnect(
        clientId: 'c1',
        protocolLevel: 5,
      ).encode();
      // Variable header layout:
      //   [0]  CONNECT type byte
      //   [1+] Remaining length (VBI — 1 byte for short payloads)
      //   [..] "MQTT" string (uint16 len + 4 bytes)
      //   [..] protocol level (1 byte)
      //   [..] connect flags (1 byte)
      //   [..] keep-alive (uint16 BE)
      //   [..] properties block (VBI len = 0 here)
      // Find the protocol level byte: after 1-byte type + 1-byte
      // remaining-length + 6-byte "MQTT" string = offset 8.
      expect(bytes[0], MqttPacketType.connect);
      expect(bytes[8], 0x05); // protocol level = 5
      // Connect flags + keepalive(2) → bytes 9..11; then properties
      // length VBI at byte 12 must be 0x00 (empty).
      expect(bytes[12], 0x00);
    });

    test('TC-CON5-002 protocolLevel 4 default keeps the 3.1.1 wire layout',
        () {
      final bytes = const MqttConnect(clientId: 'c1').encode();
      expect(bytes[8], 0x04);
      // Byte 12 is the next field on the payload (clientId length hi
      // byte) — there is NO properties block.
      expect(bytes[12], 0x00); // clientId length (high byte) == 0
      expect(bytes[13], 0x02); // clientId length (low byte) == 2
    });

    test('TC-CON5-003 v5 properties block emitted with mixed types', () {
      final bytes = MqttConnect(
        clientId: 'c1',
        protocolLevel: 5,
        properties: [
          MqttUint32Property(MqttPropertyId.sessionExpiryInterval, 3600),
          MqttUint16Property(MqttPropertyId.receiveMaximum, 100),
          MqttUserProperty('app', 'mcp_io'),
        ],
      ).encode();
      expect(bytes[0], MqttPacketType.connect);
      expect(bytes[8], 0x05);
      // The property block at byte 12+ should not be empty: the VBI
      // length byte must be > 0.
      expect(bytes[12], greaterThan(0));
    });

    test('TC-CON5-004 v5 mode emits will-properties block ahead of will '
        'topic when willTopic is set', () {
      final bytes = MqttConnect(
        clientId: 'c1',
        protocolLevel: 5,
        willTopic: 'status',
        willPayload: const [0x4F, 0x4B], // "OK"
        willProperties: [
          MqttUint32Property(MqttPropertyId.willDelayInterval, 30),
        ],
      ).encode();
      // Encoding succeeds and the wire bytes contain a non-empty
      // payload (clientId + willProps + willTopic + willPayload).
      expect(bytes.length, greaterThan(20));
    });

    test('TC-CON5-005 unsupported protocolLevel throws', () {
      expect(
        () => const MqttConnect(clientId: 'c1', protocolLevel: 7).encode(),
        throwsA(isA<MqttCodecError>()),
      );
    });
  });

  group('MqttConnack v5 mode', () {
    test('TC-CNA5-001 fromBodyV5 decodes ack flags + reason code + props',
        () {
      // Body: [0x01 sessionPresent, 0x00 reasonCode, properties block].
      // Properties: receiveMaximum = 50.
      final propBlock = encodeProperties([
        MqttUint16Property(MqttPropertyId.receiveMaximum, 50),
      ]);
      final body = [0x01, 0x00, ...propBlock];
      final ack = MqttConnack.fromBodyV5(body);
      expect(ack.sessionPresent, isTrue);
      expect(ack.returnCode, 0);
      expect(ack.properties, hasLength(1));
      final p = ack.properties.first as MqttUint16Property;
      expect(p.id, MqttPropertyId.receiveMaximum);
      expect(p.value, 50);
    });

    test('TC-CNA5-002 fromBodyV5 with empty properties block', () {
      final body = [0x00, 0x00, 0x00];
      final ack = MqttConnack.fromBodyV5(body);
      expect(ack.properties, isEmpty);
      expect(ack.returnCode, 0);
    });

    test('TC-CNA5-003 v3 fromBody still works (BC)', () {
      const body = [0x00, 0x05]; // refused: not authorized
      final ack = MqttConnack.fromBody(body);
      expect(ack.returnCode, 5);
      expect(ack.properties, isEmpty);
    });
  });
}

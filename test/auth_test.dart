/// Tests for MQTT v5 AUTH packet (spec §3.15) — `MqttAuth` +
/// `encodeAuth` + `MqttAuth.fromBody` + reason code constants.
@TestOn('vm')
library;

import 'package:mcp_io_mqtt/mcp_io_mqtt.dart';
import 'package:test/test.dart';

void main() {
  group('AUTH encode (v5 only)', () {
    test('TC-AUTH-001 short form when reasonCode=0 + no properties', () {
      final bytes = encodeAuth();
      expect(bytes, [MqttPacketType.auth, 0x00]);
    });

    test('TC-AUTH-002 reasonCode 0x18 + auth method property', () {
      final bytes = encodeAuth(
        reasonCode: MqttAuthReason.continueAuthentication,
        properties: [
          MqttStringProperty(
              MqttPropertyId.authenticationMethod, 'SCRAM-SHA-256'),
          MqttBinaryDataProperty(
              MqttPropertyId.authenticationData, [0x01, 0x02, 0x03]),
        ],
      );
      // Fixed header
      expect(bytes[0], MqttPacketType.auth);
      // Body: reasonCode 0x18 + properties block
      // properties block starts after remaining-length
      final remaining = bytes[1];
      expect(remaining, greaterThan(2));
      expect(bytes[2], 0x18);
    });

    test('TC-AUTH-003 v3.1.1 protocolLevel rejected', () {
      expect(
        () => encodeAuth(protocolLevel: 4),
        throwsA(isA<MqttCodecError>()),
      );
    });
  });

  group('AUTH decode', () {
    test('TC-AUTH-004 empty body → reasonCode 0 + no properties', () {
      final auth = MqttAuth.fromBody(const []);
      expect(auth.reasonCode, 0);
      expect(auth.properties, isEmpty);
    });

    test('TC-AUTH-005 single-byte body → reasonCode only', () {
      final auth = MqttAuth.fromBody([0x19]);
      expect(auth.reasonCode, MqttAuthReason.reAuthenticate);
      expect(auth.properties, isEmpty);
    });

    test('TC-AUTH-006 reasonCode + properties roundtrip', () {
      final encoded = encodeAuth(
        reasonCode: MqttAuthReason.continueAuthentication,
        properties: [
          MqttStringProperty(
              MqttPropertyId.authenticationMethod, 'SCRAM-SHA-256'),
        ],
      );
      // Skip fixed header (1) + remaining length VBI byte (1).
      final body = encoded.sublist(2);
      final auth = MqttAuth.fromBody(body);
      expect(auth.reasonCode, MqttAuthReason.continueAuthentication);
      expect(auth.properties.length, 1);
      final prop = auth.properties.first as MqttStringProperty;
      expect(prop.id, MqttPropertyId.authenticationMethod);
      expect(prop.value, 'SCRAM-SHA-256');
    });
  });

  group('Reason code constants', () {
    test('TC-AUTH-007 success/continue/re-authenticate values', () {
      expect(MqttAuthReason.success, 0x00);
      expect(MqttAuthReason.continueAuthentication, 0x18);
      expect(MqttAuthReason.reAuthenticate, 0x19);
    });
  });

  group('Packet type constant', () {
    test('TC-AUTH-008 MqttPacketType.auth = 0xF0 (control type 15)', () {
      expect(MqttPacketType.auth, 0xF0);
    });
  });
}

import 'package:mcp_io_mqtt/mcp_io_mqtt.dart';
import 'package:test/test.dart';

void main() {
  group('VariableByteInteger', () {
    test('TC-VBI-001 1-byte values 0..127', () {
      expect(VariableByteInteger.encode(0), [0]);
      expect(VariableByteInteger.encode(127), [127]);
    });

    test('TC-VBI-002 2-byte boundary 128', () {
      // 128 = 0x80 → continuation bit + 0x01
      expect(VariableByteInteger.encode(128), [0x80, 0x01]);
      expect(VariableByteInteger.encode(16383), [0xFF, 0x7F]);
    });

    test('TC-VBI-003 3-byte boundary 16384', () {
      expect(VariableByteInteger.encode(16384), [0x80, 0x80, 0x01]);
    });

    test('TC-VBI-004 4-byte max 268435455', () {
      expect(VariableByteInteger.encode(268435455),
          [0xFF, 0xFF, 0xFF, 0x7F]);
    });

    test('TC-VBI-005 roundtrip across boundaries', () {
      for (final v in [0, 1, 127, 128, 16383, 16384, 268435455]) {
        final encoded = VariableByteInteger.encode(v);
        final decoded = VariableByteInteger.decode(encoded);
        expect(decoded.value, v);
        expect(decoded.length, encoded.length);
      }
    });

    test('TC-VBI-006 out of range throws', () {
      expect(() => VariableByteInteger.encode(-1), throwsArgumentError);
      expect(() => VariableByteInteger.encode(0x10000000), throwsArgumentError);
    });

    test('TC-VBI-007 truncated throws', () {
      expect(() => VariableByteInteger.decode([0x80]), throwsFormatException);
    });
  });

  group('MqttControlPacketType', () {
    test('TC-CPT-001 from high nibble', () {
      expect(MqttControlPacketType.fromHighNibble(0x10),
          MqttControlPacketType.connect);
      expect(MqttControlPacketType.fromHighNibble(0x30),
          MqttControlPacketType.publish);
      expect(MqttControlPacketType.fromHighNibble(0xE0),
          MqttControlPacketType.disconnect);
    });

    test('TC-CPT-002 fixed flags for PUBREL/SUBSCRIBE/UNSUBSCRIBE', () {
      expect(MqttControlPacketType.pubRel.fixedFlags, 0x02);
      expect(MqttControlPacketType.subscribe.fixedFlags, 0x02);
      expect(MqttControlPacketType.unsubscribe.fixedFlags, 0x02);
    });
  });

  group('PublishFlags', () {
    test('TC-PUB-001 toFlagsByte / fromFlagsByte roundtrip', () {
      const a = PublishFlags(qos: MqttQos.atLeastOnce, retain: true);
      final byte = a.toFlagsByte();
      final b = PublishFlags.fromFlagsByte(byte);
      expect(b.qos, MqttQos.atLeastOnce);
      expect(b.retain, isTrue);
      expect(b.dup, isFalse);
    });

    test('TC-PUB-002 dup + qos2 + retain', () {
      const a = PublishFlags(dup: true, qos: MqttQos.exactlyOnce, retain: true);
      final byte = a.toFlagsByte();
      // dup=1 qos=10 retain=1 → 1101 binary = 0xD
      expect(byte, 0x0D);
    });
  });

  group('MqttPropertyId', () {
    test('TC-PROP-001 known ids', () {
      expect(MqttPropertyId.fromId(0x21), MqttPropertyId.receiveMaximum);
      expect(MqttPropertyId.fromId(0x26), MqttPropertyId.userProperty);
    });

    test('TC-PROP-002 unknown id throws', () {
      expect(() => MqttPropertyId.fromId(0xFF), throwsFormatException);
    });
  });

  group('MqttReasonCode', () {
    test('TC-RC-001 success vs error split at 0x80', () {
      expect(MqttReasonCode.success.isSuccess, isTrue);
      expect(MqttReasonCode.unspecifiedError.isError, isTrue);
      expect(MqttReasonCode.banned.isError, isTrue);
    });
  });

  group('ReasonToIoError', () {
    test('TC-RTI-001 success returns null', () {
      expect(ReasonToIoError.fromReasonByte(0x00), isNull);
      expect(ReasonToIoError.fromReasonByte(0x10), isNull);
    });

    test('TC-RTI-002 client errors mapped', () {
      expect(ReasonToIoError.fromReasonByte(0x87)!.code,
          'auth.not_authorized');
      expect(ReasonToIoError.fromReasonByte(0x97)!.code,
          'quota.exceeded');
    });

    test('TC-RTI-003 reconnect required for transport-level', () {
      expect(ReasonToIoError.requiresReconnect(0x8D), isTrue); // KeepAlive
      expect(ReasonToIoError.requiresReconnect(0x88), isTrue); // unavailable
      expect(ReasonToIoError.requiresReconnect(0x9D), isTrue); // server moved
      expect(ReasonToIoError.requiresReconnect(0x87), isFalse); // not authorized
    });
  });
}

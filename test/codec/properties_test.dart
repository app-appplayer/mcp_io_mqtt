import 'package:mcp_io_mqtt/mcp_io_mqtt.dart';
import 'package:test/test.dart';

void main() {
  group('encodeProperties / decodeProperties roundtrip', () {
    test('TC-PROP-001 empty list encodes to a single 0x00 length byte', () {
      final encoded = encodeProperties([]);
      expect(encoded, [0x00]);
      final back = decodeProperties(encoded, 0);
      expect(back.props, isEmpty);
      expect(back.length, 1);
    });

    test('TC-PROP-002 byte property — payloadFormatIndicator', () {
      final props = [
        MqttByteProperty(MqttPropertyId.payloadFormatIndicator, 1),
      ];
      final encoded = encodeProperties(props);
      // Wire: VBI len(2) + id(0x01) + value(0x01).
      expect(encoded, [0x02, 0x01, 0x01]);
      final back = decodeProperties(encoded, 0);
      expect(back.props, hasLength(1));
      final p = back.props.first as MqttByteProperty;
      expect(p.id, MqttPropertyId.payloadFormatIndicator);
      expect(p.value, 1);
    });

    test('TC-PROP-003 uint16 property — receiveMaximum', () {
      final props = [
        MqttUint16Property(MqttPropertyId.receiveMaximum, 0x0123),
      ];
      final encoded = encodeProperties(props);
      // VBI len(3) + id(0x21) + uint16 BE(0x01, 0x23).
      expect(encoded, [0x03, 0x21, 0x01, 0x23]);
      final back = decodeProperties(encoded, 0);
      expect((back.props.first as MqttUint16Property).value, 0x0123);
    });

    test('TC-PROP-004 uint32 property — sessionExpiryInterval', () {
      final props = [
        MqttUint32Property(
            MqttPropertyId.sessionExpiryInterval, 0xAABBCCDD),
      ];
      final encoded = encodeProperties(props);
      expect(encoded.sublist(2),
          [0xAA, 0xBB, 0xCC, 0xDD]); // big-endian
      final back = decodeProperties(encoded, 0);
      expect((back.props.first as MqttUint32Property).value, 0xAABBCCDD);
    });

    test('TC-PROP-005 variable-int property — subscriptionIdentifier', () {
      final props = [
        MqttVariableIntProperty(
            MqttPropertyId.subscriptionIdentifier, 200),
      ];
      // 200 = 0xC8 → VBI: [0xC8, 0x01] (2 bytes).
      final encoded = encodeProperties(props);
      // Block: VBI len(3) + id(0x0B) + 200 as VBI(2).
      expect(encoded.sublist(1, 2), [0x0B]);
      final back = decodeProperties(encoded, 0);
      expect((back.props.first as MqttVariableIntProperty).value, 200);
    });

    test('TC-PROP-006 utf8 string property — contentType', () {
      final props = [
        MqttStringProperty(MqttPropertyId.contentType, 'text/plain'),
      ];
      final encoded = encodeProperties(props);
      final back = decodeProperties(encoded, 0);
      expect((back.props.first as MqttStringProperty).value, 'text/plain');
    });

    test('TC-PROP-007 binary data property — correlationData', () {
      final raw = [for (var i = 0; i < 16; i++) i & 0xFF];
      final props = [
        MqttBinaryDataProperty(MqttPropertyId.correlationData, raw),
      ];
      final encoded = encodeProperties(props);
      final back = decodeProperties(encoded, 0);
      expect((back.props.first as MqttBinaryDataProperty).value, raw);
    });

    test('TC-PROP-008 user property — repeated key/value pairs', () {
      final props = [
        MqttUserProperty('region', 'apne1'),
        MqttUserProperty('tenant', 'acme'),
        MqttUserProperty('region', 'apne2'), // repeated key allowed
      ];
      final encoded = encodeProperties(props);
      final back = decodeProperties(encoded, 0);
      expect(back.props, hasLength(3));
      final ups = back.props.cast<MqttUserProperty>();
      expect(ups[0].key, 'region');
      expect(ups[0].value, 'apne1');
      expect(ups[1].key, 'tenant');
      expect(ups[2].key, 'region');
      expect(ups[2].value, 'apne2');
    });

    test('TC-PROP-009 mixed property block roundtrip', () {
      final props = [
        MqttUint32Property(MqttPropertyId.sessionExpiryInterval, 3600),
        MqttUint16Property(MqttPropertyId.receiveMaximum, 100),
        MqttStringProperty(MqttPropertyId.contentType, 'application/json'),
        MqttUserProperty('app', 'mcp_io'),
      ];
      final encoded = encodeProperties(props);
      final back = decodeProperties(encoded, 0);
      expect(back.props, hasLength(4));
      expect((back.props[0] as MqttUint32Property).value, 3600);
      expect((back.props[1] as MqttUint16Property).value, 100);
      expect((back.props[2] as MqttStringProperty).value, 'application/json');
      expect((back.props[3] as MqttUserProperty).key, 'app');
    });
  });

  group('decode error paths', () {
    test('TC-PROP-010 truncated block reports failure', () {
      // Declare 10 bytes of body but only ship 2.
      expect(
        () => decodeProperties([0x0A, 0x21, 0x00], 0),
        throwsFormatException,
      );
    });

    test('TC-PROP-011 unknown property id throws', () {
      // Block len 2, id 0x99 (unassigned), 1-byte value.
      expect(
        () => decodeProperties([0x02, 0x99, 0x01], 0),
        throwsFormatException,
      );
    });

    test('TC-PROP-012 declared length mismatched', () {
      // Block len says 5 but the entries only consume 3 bytes (id 0x01
      // + 1-byte value + 1 stray = 3 actually) — easier mismatch:
      // decode is asked with a length that overshoots.
      expect(
        () => decodeProperties([0xFF, 0xFF, 0xFF, 0x7F], 0),
        throwsFormatException,
      );
    });
  });

  group('Wire offset semantics', () {
    test('TC-PROP-013 decodeProperties consumes only the block bytes', () {
      // Compose a block followed by trailing garbage. Decode must
      // report the block length and leave the trailer untouched.
      final block = encodeProperties([
        MqttByteProperty(MqttPropertyId.payloadFormatIndicator, 1),
      ]);
      final buffer = [...block, 0xCA, 0xFE, 0xBA, 0xBE];
      final r = decodeProperties(buffer, 0);
      expect(r.length, block.length);
      expect(buffer.length - r.length, 4);
    });

    test('TC-PROP-014 decodeProperties honours non-zero offset', () {
      final prefix = [0xAA, 0xBB];
      final block = encodeProperties([
        MqttUint16Property(MqttPropertyId.serverKeepAlive, 60),
      ]);
      final buffer = [...prefix, ...block];
      final r = decodeProperties(buffer, prefix.length);
      expect((r.props.first as MqttUint16Property).value, 60);
      expect(r.length, block.length);
    });
  });
}

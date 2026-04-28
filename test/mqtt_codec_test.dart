import 'dart:convert';
import 'dart:typed_data';

import 'package:mcp_io_mqtt/src/mqtt_codec.dart';
import 'package:test/test.dart';

void main() {
  group('remaining length varint', () {
    test('round-trip for boundary values', () {
      for (final value in const [0, 1, 127, 128, 16383, 16384, 2097151, 2097152]) {
        final encoded = encodeRemainingLength(value);
        final decoded = decodeRemainingLength(encoded, 0);
        expect(decoded.value, value);
        expect(decoded.bytesRead, encoded.length);
      }
    });

    test('rejects out-of-range value', () {
      expect(() => encodeRemainingLength(-1), throwsA(isA<MqttCodecError>()));
      expect(
        () => encodeRemainingLength(0x10000000),
        throwsA(isA<MqttCodecError>()),
      );
    });

    test('decode reports incomplete on truncation', () {
      expect(
        () => decodeRemainingLength([0x80], 0),
        throwsA(isA<MqttCodecError>()),
      );
    });
  });

  group('MQTT utf-8 string codec', () {
    test('round-trip preserves content', () {
      final bytes = encodeMqttString('hello MQTT');
      expect(bytes[0], 0);
      expect(bytes[1], 'hello MQTT'.length);
      final decoded = decodeMqttString(bytes, 0);
      expect(decoded.value, 'hello MQTT');
      expect(decoded.bytesRead, bytes.length);
    });

    test('handles multibyte characters', () {
      final bytes = encodeMqttString('한글과 🚀');
      final decoded = decodeMqttString(bytes, 0);
      expect(decoded.value, '한글과 🚀');
    });
  });

  group('CONNECT encoding', () {
    test('minimal CONNECT packet layout', () {
      final bytes = const MqttConnect(
        clientId: 'c1', keepAliveSeconds: 30,
      ).encode();
      // Header byte for CONNECT.
      expect(bytes[0], MqttPacketType.connect);
      // Parse through tryParsePacket for round-trip framing.
      final parsed = tryParsePacket(bytes)!;
      expect(parsed.bytesRead, bytes.length);
      expect(parsed.packet.packetType, MqttPacketType.connect);
      // Variable header begins with "MQTT" string + level 4.
      final body = parsed.packet.body;
      expect(decodeMqttString(body, 0).value, 'MQTT');
    });

    test('CONNECT with username/password sets flags', () {
      final bytes = const MqttConnect(
        clientId: 'c', username: 'u', password: 'p',
      ).encode();
      final parsed = tryParsePacket(bytes)!;
      // Flags live after ("MQTT"=6 bytes) + protocolLevel=1 = index 7.
      final flagsByte = parsed.packet.body[7];
      expect((flagsByte & 0x80) != 0, isTrue, reason: 'username flag');
      expect((flagsByte & 0x40) != 0, isTrue, reason: 'password flag');
      expect((flagsByte & 0x02) != 0, isTrue, reason: 'clean session flag');
    });
  });

  group('PUBLISH encoding / decoding', () {
    test('QoS 0 round-trip', () {
      final pub = MqttPublish(
        topic: 'sensors/temp',
        payload: utf8.encode('23.5'),
      );
      final bytes = pub.encode();
      final parsed = tryParsePacket(bytes)!;
      expect(parsed.packet.packetType, MqttPacketType.publish);
      final decoded = MqttPublish.fromBody(
        parsed.packet.headerByte, parsed.packet.body,
      );
      expect(decoded.topic, 'sensors/temp');
      expect(decoded.payload, utf8.encode('23.5'));
      expect(decoded.qos, 0);
      expect(decoded.retain, isFalse);
      expect(decoded.packetId, isNull);
    });

    test('retain flag is preserved in header byte', () {
      final pub = MqttPublish(
        topic: 't', payload: const [0x01],
        retain: true,
      );
      final bytes = pub.encode();
      final parsed = tryParsePacket(bytes)!;
      expect(parsed.packet.headerByte & 0x01, 0x01);
      final decoded = MqttPublish.fromBody(
        parsed.packet.headerByte, parsed.packet.body,
      );
      expect(decoded.retain, isTrue);
    });

    test('encoding QoS > 0 throws (unsupported in this version)', () {
      expect(
        () => MqttPublish(
          topic: 't', payload: const [], qos: 1,
        ).encode(),
        throwsA(isA<MqttCodecError>()),
      );
    });
  });

  group('SUBSCRIBE encoding', () {
    test('reserved flags == 0010', () {
      final bytes = const MqttSubscribe(
        packetId: 7,
        entries: [MqttSubscribeEntry(filter: 'sensors/+/temperature')],
      ).encode();
      expect(bytes[0] & 0x0F, 0x02);
    });

    test('multiple filter entries serialized sequentially', () {
      final bytes = const MqttSubscribe(
        packetId: 10,
        entries: [
          MqttSubscribeEntry(filter: 'a'),
          MqttSubscribeEntry(filter: 'b/#'),
        ],
      ).encode();
      final parsed = tryParsePacket(bytes)!;
      final body = parsed.packet.body;
      // packetId is the first two bytes.
      expect((body[0] << 8) | body[1], 10);
    });
  });

  group('SUBACK decoding', () {
    test('returnCodes preserved', () {
      final ack = MqttSuback.fromBody(const [0x00, 0x05, 0x00, 0x01, 0x02]);
      expect(ack.packetId, 5);
      expect(ack.returnCodes, [0, 1, 2]);
    });
  });

  group('topic match wildcards', () {
    test('exact match', () {
      expect(topicMatches('a/b/c', 'a/b/c'), isTrue);
      expect(topicMatches('a/b/c', 'a/b'), isFalse);
    });

    test('+ matches exactly one level', () {
      expect(topicMatches('sensors/+/temperature', 'sensors/room1/temperature'), isTrue);
      expect(topicMatches('sensors/+/temperature', 'sensors/temperature'), isFalse);
      expect(
        topicMatches('sensors/+/+', 'sensors/room1/temperature'),
        isTrue,
      );
    });

    test('# matches zero or more trailing levels', () {
      expect(topicMatches('a/#', 'a/b/c/d'), isTrue);
      expect(topicMatches('a/#', 'a'), isTrue,
        reason: 'per MQTT v3.1.1 § 4.7.1.2, sport/tennis/player1/# matches '
            'sport/tennis/player1 (zero trailing levels).');
      expect(topicMatches('#', 'literally/anything'), isTrue);
    });

    test('# must be the last segment', () {
      expect(topicMatches('a/#/c', 'a/b/c'), isFalse);
    });
  });

  group('tryParsePacket framing', () {
    test('returns null when buffer is short', () {
      final full = const MqttConnect(clientId: 'c').encode();
      expect(tryParsePacket(full.sublist(0, 3)), isNull);
    });

    test('returns packet + bytesRead on full frame', () {
      final pub = MqttPublish(topic: 't', payload: const [1, 2]).encode();
      final parsed = tryParsePacket(pub);
      expect(parsed, isNotNull);
      expect(parsed!.bytesRead, pub.length);
    });

    test('handles two back-to-back packets in one buffer', () {
      final a = encodePingReq();
      final b = encodeDisconnect();
      final combined = Uint8List.fromList([...a, ...b]);
      final first = tryParsePacket(combined)!;
      expect(first.packet.packetType, MqttPacketType.pingreq);
      final second = tryParsePacket(combined, offset: first.bytesRead)!;
      expect(second.packet.packetType, MqttPacketType.disconnect);
    });
  });
}

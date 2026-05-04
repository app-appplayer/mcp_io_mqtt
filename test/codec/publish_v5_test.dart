import 'package:mcp_bundle/mcp_bundle.dart';
import 'package:mcp_io_mqtt/mcp_io_mqtt.dart';
import 'package:test/test.dart';

void main() {
  group('MqttPublish v5 mode — encode', () {
    test('TC-PUB5-001 protocolLevel=5 emits empty property block when '
        'properties list is empty', () {
      final bytes = MqttPublish(
        topic: 't',
        payload: const [0x01],
        protocolLevel: 5,
      ).encode();
      // Variable header: topic (uint16 length + 1 char) + properties
      // length VBI (0x00 here) + payload.
      // Body offset starts after type byte + remaining-length VBI.
      // For this short packet remaining-length is 1 byte.
      // Expected layout: type, remLen, 0x00, 0x01, 't'-byte, 0x00, 0x01.
      expect(bytes[0], MqttPacketType.publish);
      // After topic length(2)+topic(1)+propLen(1) = 4 bytes, payload
      // begins. The byte before payload (= properties length VBI)
      // should be 0x00.
      expect(bytes[bytes.length - 2], 0x00); // property block length
      expect(bytes.last, 0x01); // payload byte
    });

    test('TC-PUB5-002 v5 PUBLISH with topicAlias + contentType + userProp '
        'roundtrips through fromBodyV5', () {
      final encoded = MqttPublish(
        topic: 'sensors/x',
        payload: 'hello'.codeUnits,
        protocolLevel: 5,
        properties: [
          MqttUint16Property(MqttPropertyId.topicAlias, 7),
          MqttStringProperty(
              MqttPropertyId.contentType, 'application/json'),
          MqttUserProperty('source', 'pi-01'),
        ],
      ).encode();

      final parsed = tryParsePacket(encoded)!.packet;
      expect(parsed.packetType, MqttPacketType.publish);

      final pub = MqttPublish.fromBodyV5(parsed.headerByte, parsed.body);
      expect(pub.topic, 'sensors/x');
      expect(pub.payload, 'hello'.codeUnits);
      expect(pub.properties, hasLength(3));
      expect(
        (pub.properties.firstWhere((p) => p.id == MqttPropertyId.topicAlias)
                as MqttUint16Property)
            .value,
        7,
      );
      expect(
        (pub.properties.firstWhere((p) => p.id == MqttPropertyId.contentType)
                as MqttStringProperty)
            .value,
        'application/json',
      );
    });

    test('TC-PUB5-003 v5 QoS 1 PUBLISH carries packetId AND properties',
        () {
      final encoded = MqttPublish(
        topic: 'a/b',
        payload: const [0xCA, 0xFE],
        qos: 1,
        packetId: 42,
        protocolLevel: 5,
        properties: [
          MqttByteProperty(MqttPropertyId.payloadFormatIndicator, 1),
        ],
      ).encode();
      final parsed = tryParsePacket(encoded)!.packet;
      expect(parsed.packetType, MqttPacketType.publish);
      // Decode v5 — verify both packetId and the property.
      final pub = MqttPublish.fromBodyV5(parsed.headerByte, parsed.body);
      expect(pub.qos, 1);
      expect(pub.packetId, 42);
      expect(pub.properties, hasLength(1));
      final pi = pub.properties.first as MqttByteProperty;
      expect(pi.id, MqttPropertyId.payloadFormatIndicator);
      expect(pi.value, 1);
      expect(pub.payload, [0xCA, 0xFE]);
    });

    test(
        'TC-PUB5-004 fromBodyV5 on a frame produced WITHOUT properties '
        'reads the property block as 0-length and the rest as payload',
        () {
      // Encode v3.1.1-style (no property block) but parse with v5 —
      // the first body byte must be 0x00 (or downstream payload).
      // To make this deterministic we wrap the v3 PUBLISH and then
      // hand-craft a small property block at the right offset.
      // Easier: encode v5 with empty props.
      final encoded = MqttPublish(
        topic: 't',
        payload: const [0x09],
        protocolLevel: 5,
      ).encode();
      final parsed = tryParsePacket(encoded)!.packet;
      final pub = MqttPublish.fromBodyV5(parsed.headerByte, parsed.body);
      expect(pub.properties, isEmpty);
      expect(pub.payload, [0x09]);
    });

    test('TC-PUB5-005 unsupported protocolLevel throws', () {
      expect(
        () => MqttPublish(
          topic: 't', payload: const [], protocolLevel: 7,
        ).encode(),
        throwsA(isA<MqttCodecError>()),
      );
    });
  });

  group('MqttAdapter v5 publish properties wiring', () {
    test('TC-PUB5-A-001 mqtt.publish args.properties is forwarded into '
        'the encoded PUBLISH frame', () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker',
        clientId: 'c1',
        transport: transport,
        protocolLevel: 5,
      );
      // Open the connection (v5 CONNACK).
      final fut = adapter.connect();
      await Future<void>.delayed(Duration.zero);
      transport.injectIncoming(const MqttIncomingPacket(
        headerByte: MqttPacketType.connack,
        body: [0x00, 0x00, 0x00], // empty v5 props
      ));
      await fut;

      final r = await adapter.execute(Command(
        action: 'mqtt.publish', target: 'sensors/x',
        args: {
          'payload': 'hello',
          'qos': 0,
          'properties': [
            MqttStringProperty(MqttPropertyId.contentType, 'text/plain'),
            MqttUserProperty('region', 'apne1'),
          ],
        },
      ));
      expect(r.status, CommandStatus.completed);

      // Outbound PUBLISH is the second sent packet (after CONNECT).
      final pub = transport.parseSent(1);
      expect(pub.packetType, MqttPacketType.publish);
      final decoded = MqttPublish.fromBodyV5(pub.headerByte, pub.body);
      expect(decoded.topic, 'sensors/x');
      expect(decoded.properties, hasLength(2));
      final ct = decoded.properties
          .whereType<MqttStringProperty>()
          .firstWhere((p) => p.id == MqttPropertyId.contentType);
      expect(ct.value, 'text/plain');

      await adapter.disconnect();
    });
  });
}

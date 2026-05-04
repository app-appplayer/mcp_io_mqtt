/// `MqttAdapter` v5 CONNECT/CONNACK integration test.
///
/// The in-memory transport injects a v5 CONNACK with server-supplied
/// properties; the adapter decodes them and exposes via `lastConnack`.
library;

import 'dart:typed_data';

import 'package:mcp_bundle/mcp_bundle.dart';
import 'package:mcp_io_mqtt/mcp_io_mqtt.dart';
import 'package:test/test.dart';

void main() {
  group('MqttAdapter v5 CONNECT/CONNACK', () {
    test('TC-V5C-001 v5 CONNECT carries protocolLevel=5 + properties block',
        () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker',
        clientId: 'c1',
        transport: transport,
        protocolLevel: 5,
        connectProperties: [
          MqttUint32Property(MqttPropertyId.sessionExpiryInterval, 3600),
          MqttUserProperty('app', 'mcp_io'),
        ],
      );
      // Inject CONNACK after CONNECT has been sent.
      final fut = adapter.connect();
      await Future<void>.delayed(Duration.zero);
      // v5 CONNACK: ack flags (1) + reason code (1) + property block.
      final propBlock = encodeProperties([
        MqttUint16Property(MqttPropertyId.receiveMaximum, 50),
      ]);
      transport.injectIncoming(MqttIncomingPacket(
        headerByte: MqttPacketType.connack,
        body: [0x00, 0x00, ...propBlock],
      ));
      await fut;

      // Verify outbound CONNECT framing.
      final connect = transport.parseSent(0);
      expect(connect.packetType, MqttPacketType.connect);
      // Body contains the "MQTT" name + protocol level 0x05 + flags +
      // keep-alive + property block. The protocol level byte sits at
      // offset 6 inside the body (uint16 string length 4 + 4 chars).
      expect(connect.body[6], 0x05);

      // Adapter exposed the server-side properties.
      final ack = adapter.lastConnack!;
      expect(ack.returnCode, 0);
      expect(ack.properties, hasLength(1));
      final rmax = ack.properties.first as MqttUint16Property;
      expect(rmax.id, MqttPropertyId.receiveMaximum);
      expect(rmax.value, 50);

      await adapter.disconnect();
    });

    test('TC-V5C-002 v3.1.1 default keeps the original CONNECT layout (BC)',
        () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker',
        clientId: 'c1',
        transport: transport,
      );
      final fut = adapter.connect();
      await Future<void>.delayed(Duration.zero);
      transport.injectIncoming(const MqttIncomingPacket(
        headerByte: MqttPacketType.connack,
        body: [0x00, 0x00], // v3 — return code 0
      ));
      await fut;

      final connect = transport.parseSent(0);
      expect(connect.body[6], 0x04);
      expect(adapter.lastConnack!.properties, isEmpty);
      await adapter.disconnect();
    });

    test('TC-V5C-003 will-properties block emitted when willTopic + v5',
        () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker',
        clientId: 'c1',
        transport: transport,
        protocolLevel: 5,
        willProperties: [
          MqttUint32Property(MqttPropertyId.willDelayInterval, 30),
        ],
      );
      // Set will via the existing capability before connect.
      await adapter.execute(const Command(
        action: 'mqtt.set_will', target: '',
        args: {'topic': 'status/c1', 'payload': 'offline'},
      ));
      final fut = adapter.connect();
      await Future<void>.delayed(Duration.zero);
      transport.injectIncoming(const MqttIncomingPacket(
        headerByte: MqttPacketType.connack,
        body: [0x00, 0x00, 0x00], // empty v5 properties block
      ));
      await fut;

      // The CONNECT packet should be longer than the v3.1.1
      // equivalent because of the will-properties block.
      final connect = transport.parseSent(0);
      expect(connect.body[6], 0x05);
      // Connect flags byte must have the Will flag (0x04) set.
      expect(connect.body[7] & 0x04, 0x04);

      await adapter.disconnect();
    });

    test('TC-V5C-004 v5 reason code != 0 propagates as CONNECT failure',
        () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker', clientId: 'c1',
        transport: transport, protocolLevel: 5,
      );
      // 0x87 = NotAuthorized (v5 reason code)
      Future<void>(() async {
        await Future<void>.delayed(Duration.zero);
        transport.injectIncoming(MqttIncomingPacket(
          headerByte: MqttPacketType.connack,
          body: Uint8List.fromList([0x00, 0x87, 0x00]),
        ));
      });
      await expectLater(adapter.connect(), throwsStateError);
    });
  });
}

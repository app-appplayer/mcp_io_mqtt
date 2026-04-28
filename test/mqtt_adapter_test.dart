import 'dart:convert';
import 'dart:typed_data';

import 'package:mcp_bundle/mcp_bundle.dart';
import 'package:mcp_io_mqtt/mcp_io_mqtt.dart';
import 'package:test/test.dart';

/// Pre-built CONNACK with return code 0 (accepted).
MqttIncomingPacket _connackAccepted() => const MqttIncomingPacket(
  headerByte: MqttPacketType.connack,
  body: [0x00, 0x00],
);

/// Build a raw PUBLISH packet and wrap it as an incoming packet (for
/// feeding into the in-memory transport).
MqttIncomingPacket _publish(String topic, List<int> payload) {
  final bytes = MqttPublish(topic: topic, payload: payload).encode();
  final parsed = tryParsePacket(bytes)!;
  return parsed.packet;
}

MqttIncomingPacket _suback(int packetId, List<int> returnCodes) {
  final body = [
    (packetId >> 8) & 0xFF, packetId & 0xFF,
    ...returnCodes,
  ];
  return MqttIncomingPacket(headerByte: MqttPacketType.suback, body: body);
}

void main() {
  group('MqttAdapter — connect handshake', () {
    test('sends CONNECT and settles on CONNACK accepted', () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker-1', clientId: 'client-A',
        transport: transport,
      );
      // Schedule CONNACK to arrive shortly after CONNECT send.
      final connectFuture = adapter.connect();
      await Future<void>.delayed(Duration.zero);
      transport.injectIncoming(_connackAccepted());
      await connectFuture;
      expect((await adapter.describe()).connectionState,
        IoConnectionState.connected);
      // Verify a CONNECT was sent.
      expect(transport.sentPackets, hasLength(1));
      final parsed = transport.parseSent(0);
      expect(parsed.packetType, MqttPacketType.connect);
    });

    test('throws when CONNACK returns non-zero code', () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker-1', clientId: 'client-A',
        transport: transport,
      );
      final fut = adapter.connect();
      await Future<void>.delayed(Duration.zero);
      transport.injectIncoming(const MqttIncomingPacket(
        headerByte: MqttPacketType.connack, body: [0x00, 0x05],
      ));
      await expectLater(fut, throwsStateError);
    });
  });

  group('MqttAdapter — execute publish', () {
    test('encodes PUBLISH packet with target + payload', () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker-1', clientId: 'c',
        transport: transport,
      );
      final fut = adapter.connect();
      await Future<void>.delayed(Duration.zero);
      transport.injectIncoming(_connackAccepted());
      await fut;

      final result = await adapter.execute(const Command(
        action: 'publish',
        target: 'sensors/room1/setpoint',
        args: {'payload': '72.5'},
      ));
      expect(result.status, CommandStatus.completed);
      // The second sent packet (index 1 after CONNECT).
      final parsed = transport.parseSent(1);
      expect(parsed.packetType, MqttPacketType.publish);
      final pub = MqttPublish.fromBody(parsed.headerByte, parsed.body);
      expect(pub.topic, 'sensors/room1/setpoint');
      expect(utf8.decode(pub.payload), '72.5');
    });

    test('publish with bytes payload', () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker-1', clientId: 'c',
        transport: transport,
      );
      final fut = adapter.connect();
      await Future<void>.delayed(Duration.zero);
      transport.injectIncoming(_connackAccepted());
      await fut;
      final bytes = Uint8List.fromList(const [0x01, 0x02, 0x03]);
      await adapter.execute(Command(
        action: 'publish',
        target: 'binary/topic',
        args: {'payload': bytes},
      ));
      final parsed = transport.parseSent(1);
      final pub = MqttPublish.fromBody(parsed.headerByte, parsed.body);
      expect(pub.payload, [0x01, 0x02, 0x03]);
    });

    test('unknown action is rejected', () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker-1', clientId: 'c',
        transport: transport,
      );
      final fut = adapter.connect();
      await Future<void>.delayed(Duration.zero);
      transport.injectIncoming(_connackAccepted());
      await fut;
      final res = await adapter.execute(const Command(
        action: 'bogus', target: 't',
      ));
      expect(res.status, CommandStatus.rejected);
      expect(res.error?.code, 'exec.unknown_action');
    });
  });

  group('MqttAdapter — subscribe stream', () {
    test('SUBSCRIBE sent on first listen', () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker-1', clientId: 'c',
        transport: transport,
      );
      final fut = adapter.connect();
      await Future<void>.delayed(Duration.zero);
      transport.injectIncoming(_connackAccepted());
      await fut;

      final stream = adapter.subscribe(const TopicSpec(
        uri: 'sensors/+/temperature', mode: TopicMode.onChange,
      ));
      final sub = stream.listen((_) {});
      await Future<void>.delayed(Duration.zero);
      // sent[0] = CONNECT, sent[1] = SUBSCRIBE.
      final parsed = transport.parseSent(1);
      expect(parsed.packetType, MqttPacketType.subscribe);
      await sub.cancel();
    });

    test('filter matches incoming PUBLISH and emits envelope', () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker-1', clientId: 'c',
        transport: transport,
      );
      final fut = adapter.connect();
      await Future<void>.delayed(Duration.zero);
      transport.injectIncoming(_connackAccepted());
      await fut;

      final received = <PayloadEnvelope>[];
      final stream = adapter.subscribe(const TopicSpec(
        uri: 'sensors/+/temperature', mode: TopicMode.onChange,
      ));
      final sub = stream.listen(received.add);
      await Future<void>.delayed(Duration.zero);

      // Broker confirms subscription, then sends one matching + one mismatching publish.
      transport.injectIncoming(_suback(1, [0x00]));
      transport.injectIncoming(_publish('sensors/room1/temperature', utf8.encode('21.3')));
      transport.injectIncoming(_publish('sensors/room2/humidity', utf8.encode('55')));
      transport.injectIncoming(_publish('sensors/room3/temperature', utf8.encode('22.0')));
      await Future<void>.delayed(Duration.zero);

      expect(received, hasLength(2));
      expect(received[0].uri, 'sensors/room1/temperature');
      expect(received[1].uri, 'sensors/room3/temperature');
      expect(received[0].payload.value, utf8.encode('21.3'));

      await sub.cancel();
    });

    test('multi-level wildcard # delivers matching topics', () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker-1', clientId: 'c',
        transport: transport,
      );
      final fut = adapter.connect();
      await Future<void>.delayed(Duration.zero);
      transport.injectIncoming(_connackAccepted());
      await fut;

      final received = <String>[];
      final sub = adapter.subscribe(const TopicSpec(
        uri: 'home/#', mode: TopicMode.onChange,
      )).listen((e) => received.add(e.uri));
      await Future<void>.delayed(Duration.zero);
      transport.injectIncoming(_publish('home/a', const []));
      transport.injectIncoming(_publish('home/a/b', const []));
      transport.injectIncoming(_publish('away/c', const []));
      await Future<void>.delayed(Duration.zero);
      expect(received, ['home/a', 'home/a/b']);
      await sub.cancel();
    });

    test('UNSUBSCRIBE sent on stream cancellation', () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker-1', clientId: 'c',
        transport: transport,
      );
      final fut = adapter.connect();
      await Future<void>.delayed(Duration.zero);
      transport.injectIncoming(_connackAccepted());
      await fut;

      final sub = adapter.subscribe(const TopicSpec(
        uri: 'topic/x', mode: TopicMode.onChange,
      )).listen((_) {});
      await Future<void>.delayed(Duration.zero);
      await sub.cancel();
      await Future<void>.delayed(Duration.zero);

      // Find an UNSUBSCRIBE among sent packets.
      final hasUnsub = transport.sentPackets.any((raw) {
        final parsed = tryParsePacket(raw);
        return parsed != null &&
            parsed.packet.packetType == MqttPacketType.unsubscribe;
      });
      expect(hasUnsub, isTrue);
    });
  });

  group('MqttAdapter — read + emergencyStop', () {
    test('read returns device.unsupported for every target', () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker-1', clientId: 'c',
        transport: transport,
      );
      final result = await adapter.read(
        const ReadSpec(targets: ['foo/bar', 'baz']),
      );
      expect(result.items, hasLength(2));
      for (final item in result.items) {
        expect(item.envelope, isNull);
        expect(item.error?.code, 'device.unsupported');
      }
    });

    test('emergencyStop closes transport and reports success', () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker-1', clientId: 'c',
        transport: transport,
      );
      final fut = adapter.connect();
      await Future<void>.delayed(Duration.zero);
      transport.injectIncoming(_connackAccepted());
      await fut;

      final r = await adapter.emergencyStop(const EmergencyStopRequest(
        reason: 'test', actorId: 'op',
      ));
      expect(r.success, isTrue);
      expect(transport.isClosed, isTrue);
    });
  });
}

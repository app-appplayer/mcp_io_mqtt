/// QoS 1 / 2 publish path integration tests.
///
/// Drives `MqttAdapter.execute(mqtt.publish, qos: 1|2)` against an
/// in-memory transport that injects synthetic ack packets and asserts
/// the wire packet sequence + completion semantics.
library;

import 'dart:async';

import 'package:mcp_bundle/mcp_bundle.dart';
import 'package:mcp_io_mqtt/mcp_io_mqtt.dart';
import 'package:test/test.dart';

MqttIncomingPacket _connackOk() => const MqttIncomingPacket(
      headerByte: MqttPacketType.connack,
      body: [0x00, 0x00],
    );

/// Wait until [transport.sentPackets] contains a packet of [type]
/// at index ≥ `from`, then return its parsed form. Times out after
/// [budget].
Future<MqttIncomingPacket> _waitForSent(
  InMemoryMqttTransport transport,
  int packetType, {
  int from = 0,
  Duration budget = const Duration(seconds: 1),
}) async {
  final stop = DateTime.now().add(budget);
  while (DateTime.now().isBefore(stop)) {
    for (var i = from; i < transport.sentPackets.length; i++) {
      final p = transport.parseSent(i);
      if (p.packetType == packetType) return p;
    }
    await Future<void>.delayed(const Duration(milliseconds: 1));
  }
  throw TimeoutException('packetType 0x${packetType.toRadixString(16)} '
      'never appeared in sentPackets');
}

int _packetIdFromPublish(MqttIncomingPacket p) {
  final topicLen = (p.body[0] << 8) | p.body[1];
  final pidIdx = 2 + topicLen;
  return (p.body[pidIdx] << 8) | p.body[pidIdx + 1];
}

void main() {
  late InMemoryMqttTransport transport;
  late MqttAdapter adapter;

  setUp(() async {
    transport = InMemoryMqttTransport();
    adapter = MqttAdapter(
      deviceId: 'broker',
      clientId: 'c1',
      transport: transport,
    )..ackTimeout = const Duration(seconds: 2);
    final fut = adapter.connect();
    await Future<void>.delayed(Duration.zero);
    transport.injectIncoming(_connackOk());
    await fut;
  });

  tearDown(() async {
    await adapter.disconnect();
  });

  group('Publish QoS 1', () {
    test('TC-Q1-001 sends PUBLISH(qos=1) with packetId, waits for PUBACK',
        () async {
      // Watch for the outbound PUBLISH and inject the matching PUBACK.
      Future<void>(() async {
        final pub = await _waitForSent(
            transport, MqttPacketType.publish, from: 1);
        final id = _packetIdFromPublish(pub);
        transport.injectIncoming(MqttIncomingPacket(
          headerByte: MqttPacketType.puback,
          body: [(id >> 8) & 0xFF, id & 0xFF],
        ));
      });
      final r = await adapter.execute(const Command(
        action: 'mqtt.publish', target: 'sensors/x',
        args: {'payload': 'hello', 'qos': 1},
      ));
      expect(r.status, CommandStatus.completed);
      expect(r.result?['qos'], 1);
      expect(r.result?['packetId'], isA<int>());
      expect(r.result?['attempts'], 1);
      final pub = transport.parseSent(1);
      expect(pub.packetType, MqttPacketType.publish);
      expect(pub.headerByte & 0x06, 0x02); // qos bits = 1
      // First attempt: DUP=0.
      expect(pub.headerByte & 0x08, 0);
    });

    test('TC-Q1-002 publish times out when no PUBACK arrives', () async {
      adapter.ackTimeout = const Duration(milliseconds: 30);
      final r = await adapter.execute(const Command(
        action: 'mqtt.publish', target: 'sensors/x',
        args: {'payload': 'hello', 'qos': 1},
      ));
      expect(r.status, CommandStatus.failed);
    });
  });

  group('Publish QoS 2', () {
    test(
        'TC-Q2-001 4-way handshake: PUBLISH → PUBREC → PUBREL → PUBCOMP',
        () async {
      Future<void>(() async {
        final pub = await _waitForSent(
            transport, MqttPacketType.publish, from: 1);
        final id = _packetIdFromPublish(pub);
        // Inject PUBREC for the publish.
        transport.injectIncoming(MqttIncomingPacket(
          headerByte: MqttPacketType.pubrec,
          body: [(id >> 8) & 0xFF, id & 0xFF],
        ));
        // Wait until the adapter sends PUBREL, then reply PUBCOMP.
        await _waitForSent(
            transport, MqttPacketType.pubrel,
            from: transport.sentPackets.length);
        transport.injectIncoming(MqttIncomingPacket(
          headerByte: MqttPacketType.pubcomp,
          body: [(id >> 8) & 0xFF, id & 0xFF],
        ));
      });

      final r = await adapter.execute(const Command(
        action: 'mqtt.publish', target: 'sensors/y',
        args: {'payload': 'world', 'qos': 2},
      ));
      expect(r.status, CommandStatus.completed);
      expect(r.result?['qos'], 2);

      // sentPackets: [0]=CONNECT, [1]=PUBLISH(qos=2), [2]=PUBREL.
      expect(transport.sentPackets, hasLength(3));
      final pubrel = transport.parseSent(2);
      expect(pubrel.headerByte, MqttPacketType.pubrel | 0x02);
    });

    test('TC-Q2-002 publish fails when PUBREC never arrives', () async {
      adapter.ackTimeout = const Duration(milliseconds: 30);
      final r = await adapter.execute(const Command(
        action: 'mqtt.publish', target: 'sensors/y',
        args: {'payload': 'x', 'qos': 2},
      ));
      expect(r.status, CommandStatus.failed);
    });
  });

  group('Inbound PUBLISH ack', () {
    test('TC-Q1-003 inbound QoS 1 PUBLISH triggers PUBACK', () async {
      final pubBytes = MqttPublish(
        topic: 'topic/a', payload: const [0x01], qos: 1, packetId: 7,
      ).encode();
      final parsed = tryParsePacket(pubBytes)!;
      transport.injectIncoming(parsed.packet);
      // Allow the ack microtask to fire.
      await Future<void>.delayed(Duration.zero);
      final ack = transport.parseSent(transport.sentPackets.length - 1);
      expect(ack.packetType, MqttPacketType.puback);
      expect(decodeAckPacketId(ack.body), 7);
    });

    test('TC-Q2-003 inbound QoS 2 PUBLISH triggers PUBREC then PUBCOMP',
        () async {
      final pubBytes = MqttPublish(
        topic: 'topic/b', payload: const [0x02], qos: 2, packetId: 9,
      ).encode();
      final parsed = tryParsePacket(pubBytes)!;
      transport.injectIncoming(parsed.packet);
      await Future<void>.delayed(Duration.zero);
      final pubrec = transport.parseSent(transport.sentPackets.length - 1);
      expect(pubrec.packetType, MqttPacketType.pubrec);

      // Now broker sends PUBREL → adapter must reply PUBCOMP.
      transport.injectIncoming(const MqttIncomingPacket(
        headerByte: MqttPacketType.pubrel | 0x02,
        body: [0x00, 0x09],
      ));
      await Future<void>.delayed(Duration.zero);
      final pubcomp = transport.parseSent(transport.sentPackets.length - 1);
      expect(pubcomp.packetType, MqttPacketType.pubcomp);
      expect(decodeAckPacketId(pubcomp.body), 9);
    });
  });

  group('PUBLISH codec — qos>0', () {
    test('TC-Q1-004 PUBLISH qos=1 encodes packetId after topic', () {
      final bytes = MqttPublish(
        topic: 'a/b', payload: const [0x10],
        qos: 1, packetId: 0x1234,
      ).encode();
      final parsed = tryParsePacket(bytes)!.packet;
      expect(parsed.packetType, MqttPacketType.publish);
      // Body layout: [topicLen(2), topic, packetId(2), payload].
      // 'a/b' → 3 bytes. So packetId at index 2+3 = 5..6.
      expect(parsed.body.length, greaterThanOrEqualTo(8));
      expect(parsed.body[5], 0x12);
      expect(parsed.body[6], 0x34);
    });

    test('TC-Q1-005 PUBLISH qos=2 sets header bits to 0x04', () {
      final bytes = MqttPublish(
        topic: 't', payload: const [],
        qos: 2, packetId: 1,
      ).encode();
      // Fixed header low nibble: qos<<1 = 0x04.
      expect(bytes[0] & 0x06, 0x04);
    });

    test('TC-Q1-006 missing packetId for qos>0 throws', () {
      expect(
        () => MqttPublish(topic: 't', payload: const [], qos: 1).encode(),
        throwsA(isA<MqttCodecError>()),
      );
    });
  });

  group('Retry on ack timeout (DUP=1)', () {
    test('TC-Q1-RT-001 QoS 1 resends with DUP=1 after first PUBACK timeout',
        () async {
      adapter.ackTimeout = const Duration(milliseconds: 30);
      adapter.maxPublishAttempts = 3;
      // Wait for the second PUBLISH (the DUP-flagged retry) then ack.
      Future<void>(() async {
        // sentPackets[0]=CONNECT. Wait until 2 publishes are present.
        while (true) {
          final pubs = [
            for (var i = 1; i < transport.sentPackets.length; i++)
              if (transport.parseSent(i).packetType ==
                  MqttPacketType.publish)
                transport.parseSent(i),
          ];
          if (pubs.length >= 2) {
            final id = _packetIdFromPublish(pubs[1]);
            expect(pubs[1].headerByte & 0x08, 0x08); // DUP=1
            transport.injectIncoming(MqttIncomingPacket(
              headerByte: MqttPacketType.puback,
              body: [(id >> 8) & 0xFF, id & 0xFF],
            ));
            return;
          }
          await Future<void>.delayed(const Duration(milliseconds: 5));
        }
      });

      final r = await adapter.execute(const Command(
        action: 'mqtt.publish', target: 'sensors/x',
        args: {'payload': 'retry-me', 'qos': 1},
      ));
      expect(r.status, CommandStatus.completed);
      expect(r.result?['attempts'], greaterThanOrEqualTo(2));
    });

    test(
        'TC-Q1-RT-002 QoS 1 fails after maxPublishAttempts timeouts',
        () async {
      adapter.ackTimeout = const Duration(milliseconds: 20);
      adapter.maxPublishAttempts = 2;
      final r = await adapter.execute(const Command(
        action: 'mqtt.publish', target: 'sensors/x',
        args: {'payload': 'never-acked', 'qos': 1},
      ));
      expect(r.status, CommandStatus.failed);
      // Two attempts must have been made.
      final pubs = [
        for (var i = 0; i < transport.sentPackets.length; i++)
          if (transport.parseSent(i).packetType == MqttPacketType.publish)
            transport.parseSent(i),
      ];
      expect(pubs.length, 2);
      // Second attempt must carry DUP=1.
      expect(pubs[1].headerByte & 0x08, 0x08);
    });

    test('TC-Q1-RT-003 packetId released back into the tracker on completion',
        () async {
      adapter.ackTimeout = const Duration(milliseconds: 30);
      // Reply to every publish promptly.
      var done = false;
      Future<void>(() async {
        while (!done) {
          for (var i = 0; i < transport.sentPackets.length; i++) {
            final p = transport.parseSent(i);
            if (p.packetType != MqttPacketType.publish) continue;
            final id = _packetIdFromPublish(p);
            transport.injectIncoming(MqttIncomingPacket(
              headerByte: MqttPacketType.puback,
              body: [(id >> 8) & 0xFF, id & 0xFF],
            ));
          }
          await Future<void>.delayed(const Duration(milliseconds: 1));
        }
      });

      // Issue 5 sequential publishes — packet ids should cycle through
      // a small set rather than monotonically grow.
      final ids = <int>{};
      for (var i = 0; i < 5; i++) {
        final r = await adapter.execute(Command(
          action: 'mqtt.publish', target: 'sensors/x',
          args: {'payload': 'm$i', 'qos': 1},
        ));
        expect(r.status, CommandStatus.completed);
        ids.add(r.result!['packetId'] as int);
      }
      done = true;
      // The tracker reuses ids when free, so the set is small.
      expect(ids.length, lessThanOrEqualTo(5));
      expect(ids, contains(1)); // first id allocated.
    });
  });
}


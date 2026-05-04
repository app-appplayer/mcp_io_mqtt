/// MQTT persistent session resume tests.
///
/// Sets up a publish that doesn't get ack'd (force timeout via short
/// `ackTimeout`), then verifies the journal still tracks it and a
/// later [resumeSession] call resends with DUP=1.
library;

import 'dart:async';

import 'package:mcp_bundle/mcp_bundle.dart';
import 'package:mcp_io_mqtt/mcp_io_mqtt.dart';
import 'package:test/test.dart';

void main() {
  group('MqttSessionJournal', () {
    test('TC-SES-001 put / contains / unackedCount / release', () {
      final j = MqttSessionJournal();
      expect(j.unackedCount, 0);
      j.put(MqttJournalEntry(
        packetId: 1,
        publish: const MqttPublish(topic: 't', payload: [], qos: 1, packetId: 1),
      ));
      expect(j.contains(1), isTrue);
      expect(j.unackedCount, 1);
      j.release(1);
      expect(j.unackedCount, 0);
    });

    test('TC-SES-002 markPubrecReceived flips stage', () {
      final j = MqttSessionJournal();
      j.put(MqttJournalEntry(
        packetId: 7,
        publish: const MqttPublish(topic: 't', payload: [], qos: 2, packetId: 7),
      ));
      expect(j.entries.first.stage, MqttPublishStage.awaitingFirstAck);
      j.markPubrecReceived(7);
      expect(j.entries.first.stage, MqttPublishStage.awaitingPubcomp);
    });

    test('TC-SES-003 clear empties the journal', () {
      final j = MqttSessionJournal();
      j.put(MqttJournalEntry(
        packetId: 3,
        publish: const MqttPublish(topic: 't', payload: [], qos: 1, packetId: 3),
      ));
      j.clear();
      expect(j.unackedCount, 0);
    });
  });

  group('MqttAdapter — session resume', () {
    test('TC-SES-004 cleanSession=true clears the journal on reconnect',
        () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker', clientId: 'c1', transport: transport,
      )..ackTimeout = const Duration(milliseconds: 30);

      // Connect with v3 sessionPresent=0.
      final fut = adapter.connect();
      await Future<void>.delayed(Duration.zero);
      transport.injectIncoming(const MqttIncomingPacket(
        headerByte: MqttPacketType.connack,
        body: [0x00, 0x00],
      ));
      await fut;

      // Publish QoS 1 that times out — journal records it.
      final r = await adapter.execute(const Command(
        action: 'mqtt.publish', target: 'sensors/x',
        args: {'payload': 'v', 'qos': 1},
      ));
      expect(r.status, CommandStatus.failed);
      expect(adapter.journal.unackedCount, 1);

      // Disconnect + reconnect with cleanSession=true (default) →
      // journal must be cleared.
      await adapter.disconnect();
      // Re-create adapter (same client, fresh transport).
      final t2 = InMemoryMqttTransport();
      final adapter2 = MqttAdapter(
        deviceId: 'broker', clientId: 'c1', transport: t2,
      );
      // Inherit journal entry by hand (simulating a host that
      // persisted state externally).
      for (final e in adapter.journal.entries) {
        adapter2.journal.put(e);
      }
      expect(adapter2.journal.unackedCount, 1);

      final fut2 = adapter2.connect();
      await Future<void>.delayed(Duration.zero);
      // Broker reports sessionPresent=0 — adapter must drop journal.
      t2.injectIncoming(const MqttIncomingPacket(
        headerByte: MqttPacketType.connack,
        body: [0x00, 0x00],
      ));
      await fut2;
      expect(adapter2.journal.unackedCount, 0);
      await adapter2.disconnect();
    });

    test('TC-SES-005 cleanSession=false + sessionPresent=1 → autoResume '
        'resends PUBLISH with DUP=1', () async {
      // Build a fresh adapter that has a pending journal entry, then
      // connect with cleanSession=false.
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker', clientId: 'c1', transport: transport,
        cleanSession: false,
      );
      adapter.journal.put(MqttJournalEntry(
        packetId: 5,
        publish: const MqttPublish(
          topic: 'sensors/x', payload: [0x01, 0x02],
          qos: 1, packetId: 5,
        ),
      ));
      expect(adapter.journal.unackedCount, 1);

      final fut = adapter.connect();
      await Future<void>.delayed(Duration.zero);
      // sessionPresent=1.
      transport.injectIncoming(const MqttIncomingPacket(
        headerByte: MqttPacketType.connack,
        body: [0x01, 0x00],
      ));
      await fut;

      // The auto-resume must have sent a PUBLISH with DUP=1.
      // sentPackets[0]=CONNECT, [1]=PUBLISH(DUP=1).
      expect(transport.sentPackets, hasLength(2));
      final replay = transport.parseSent(1);
      expect(replay.packetType, MqttPacketType.publish);
      expect(replay.headerByte & 0x08, 0x08); // DUP=1
      // Journal still has the entry — PUBACK hasn't arrived.
      expect(adapter.journal.unackedCount, 1);

      await adapter.disconnect();
    });

    test('TC-SES-006 QoS 2 stage 2 (awaitingPubcomp) replays PUBREL only',
        () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker', clientId: 'c1', transport: transport,
        cleanSession: false,
      );
      adapter.journal.put(
        MqttJournalEntry(
          packetId: 9,
          publish: const MqttPublish(
            topic: 'sensors/y', payload: [0x09],
            qos: 2, packetId: 9,
          ),
          stage: MqttPublishStage.awaitingPubcomp,
        ),
      );

      final fut = adapter.connect();
      await Future<void>.delayed(Duration.zero);
      transport.injectIncoming(const MqttIncomingPacket(
        headerByte: MqttPacketType.connack,
        body: [0x01, 0x00],
      ));
      await fut;

      // Replay must be a PUBREL, not a PUBLISH.
      expect(transport.sentPackets, hasLength(2));
      final replay = transport.parseSent(1);
      expect(replay.packetType, MqttPacketType.pubrel);

      await adapter.disconnect();
    });

    test('TC-SES-007 PUBACK on a journaled entry releases it', () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker', clientId: 'c1', transport: transport,
      );
      final fut = adapter.connect();
      await Future<void>.delayed(Duration.zero);
      transport.injectIncoming(const MqttIncomingPacket(
        headerByte: MqttPacketType.connack,
        body: [0x00, 0x00],
      ));
      await fut;

      // Watch for the outbound PUBLISH and ack it.
      Future<void>(() async {
        while (true) {
          if (transport.sentPackets.length > 1) {
            final pub = transport.parseSent(1);
            // packetId is at the topic-tail in the PUBLISH body.
            final topicLen = (pub.body[0] << 8) | pub.body[1];
            final id = (pub.body[2 + topicLen] << 8) |
                pub.body[2 + topicLen + 1];
            transport.injectIncoming(MqttIncomingPacket(
              headerByte: MqttPacketType.puback,
              body: [(id >> 8) & 0xFF, id & 0xFF],
            ));
            return;
          }
          await Future<void>.delayed(const Duration(milliseconds: 1));
        }
      });

      final r = await adapter.execute(const Command(
        action: 'mqtt.publish', target: 'sensors/x',
        args: {'payload': 'ack-me', 'qos': 1},
      ));
      expect(r.status, CommandStatus.completed);
      // After PUBACK arrives, journal entry must have been released.
      expect(adapter.journal.unackedCount, 0);
      await adapter.disconnect();
    });

    test('TC-SES-008 autoResumeOnReconnect=false leaves journal untouched',
        () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker', clientId: 'c1', transport: transport,
        cleanSession: false,
        autoResumeOnReconnect: false,
      );
      adapter.journal.put(MqttJournalEntry(
        packetId: 5,
        publish: const MqttPublish(
          topic: 't', payload: [], qos: 1, packetId: 5,
        ),
      ));
      final fut = adapter.connect();
      await Future<void>.delayed(Duration.zero);
      transport.injectIncoming(const MqttIncomingPacket(
        headerByte: MqttPacketType.connack,
        body: [0x01, 0x00], // sessionPresent=1
      ));
      await fut;

      // No automatic replay — only CONNECT was sent.
      expect(transport.sentPackets, hasLength(1));
      // Manual flush.
      final n = await adapter.resumeSession();
      expect(n, 1);
      expect(transport.sentPackets, hasLength(2));
      await adapter.disconnect();
    });
  });
}


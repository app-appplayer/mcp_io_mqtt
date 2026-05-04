/// MQTT v5 SUBSCRIBE/UNSUBSCRIBE + SUBACK/UNSUBACK property block tests.
library;

import 'package:mcp_bundle/mcp_bundle.dart';
import 'package:mcp_io_mqtt/mcp_io_mqtt.dart';
import 'package:test/test.dart';

void main() {
  group('MqttSubscribeEntry — subscription options byte', () {
    test('TC-SUB5-001 v3.1.1 byte = qos only', () {
      const e = MqttSubscribeEntry(filter: 'a/b', qos: 2);
      expect(e.encodeV3OptionsByte(), 0x02);
    });

    test('TC-SUB5-002 v5 NoLocal sets bit 2', () {
      const e = MqttSubscribeEntry(filter: 'a/b', qos: 1, noLocal: true);
      expect(e.encodeV5OptionsByte(), 0x05); // qos=1 + NL=1
    });

    test('TC-SUB5-003 v5 RetainAsPublished sets bit 3', () {
      const e = MqttSubscribeEntry(
          filter: 'a/b', qos: 0, retainAsPublished: true);
      expect(e.encodeV5OptionsByte(), 0x08);
    });

    test('TC-SUB5-004 v5 RetainHandling occupies bits 4-5', () {
      const a = MqttSubscribeEntry(
        filter: 'a/b',
        retainHandling: MqttRetainHandling.sendIfNew,
      );
      const b = MqttSubscribeEntry(
        filter: 'a/b',
        retainHandling: MqttRetainHandling.doNotSend,
      );
      expect(a.encodeV5OptionsByte(), 0x10); // 1<<4
      expect(b.encodeV5OptionsByte(), 0x20); // 2<<4
    });

    test('TC-SUB5-005 decodeOptions roundtrips every flag', () {
      const e = MqttSubscribeEntry(
        filter: 'x',
        qos: 2,
        noLocal: true,
        retainAsPublished: true,
        retainHandling: MqttRetainHandling.sendIfNew,
      );
      final byte = e.encodeV5OptionsByte();
      final back = MqttSubscribeEntry.decodeOptions(filter: 'x', byte: byte);
      expect(back.qos, 2);
      expect(back.noLocal, isTrue);
      expect(back.retainAsPublished, isTrue);
      expect(back.retainHandling, MqttRetainHandling.sendIfNew);
    });
  });

  group('MqttSubscribe v5 wire encode', () {
    test('TC-SUB5-006 v5 SUBSCRIBE has property block before filters', () {
      final encoded = MqttSubscribe(
        packetId: 1,
        protocolLevel: 5,
        entries: const [MqttSubscribeEntry(filter: 'a', qos: 0)],
        properties: [
          MqttVariableIntProperty(MqttPropertyId.subscriptionIdentifier, 7),
        ],
      ).encode();
      // The first byte of the variable header is packetId hi.
      // Find the packet via tryParsePacket and inspect the body.
      final parsed = tryParsePacket(encoded)!.packet;
      expect(parsed.packetType, MqttPacketType.subscribe);
      // Body layout: [packetIdHi, packetIdLo, propBlock, filter+optsByte].
      // packetId=1 → bytes [0x00, 0x01].
      expect(parsed.body[0], 0x00);
      expect(parsed.body[1], 0x01);
      // Property block: VBI len(2) + id(0x0B) + value(7).
      expect(parsed.body[2], 0x02); // propBlock length
      expect(parsed.body[3], 0x0B); // subscriptionIdentifier id
      expect(parsed.body[4], 0x07);
    });

    test('TC-SUB5-007 v3 default omits property block (BC)', () {
      final encoded = MqttSubscribe(
        packetId: 1,
        entries: const [MqttSubscribeEntry(filter: 'a', qos: 0)],
      ).encode();
      final parsed = tryParsePacket(encoded)!.packet;
      // Body: [0,1, 0,1, 'a', 0x00] — no property block byte.
      expect(parsed.body[0], 0x00); // pid hi
      expect(parsed.body[1], 0x01); // pid lo
      // Next bytes are filter length prefix (uint16 BE = 0x0001).
      expect(parsed.body[2], 0x00);
      expect(parsed.body[3], 0x01);
      expect(parsed.body[4], 'a'.codeUnitAt(0));
      // Last byte is the v3 options byte = qos = 0.
      expect(parsed.body.last, 0x00);
    });

    test('TC-SUB5-008 unsupported protocol level throws', () {
      expect(
        () => MqttSubscribe(
          packetId: 1,
          entries: const [MqttSubscribeEntry(filter: 'a')],
          protocolLevel: 7,
        ).encode(),
        throwsA(isA<MqttCodecError>()),
      );
    });
  });

  group('MqttSuback v5', () {
    test('TC-SUB5-009 fromBodyV5 decodes packetId + props + reason codes',
        () {
      final propBlock = encodeProperties([
        MqttUserProperty('region', 'apne1'),
      ]);
      final body = [0x00, 0x07, ...propBlock, 0x00, 0x80];
      final ack = MqttSuback.fromBodyV5(body);
      expect(ack.packetId, 7);
      expect(ack.properties, hasLength(1));
      expect(ack.returnCodes, [0x00, 0x80]);
    });

    test('TC-SUB5-010 v3 fromBody still works', () {
      final ack = MqttSuback.fromBody(const [0x00, 0x05, 0x01]);
      expect(ack.packetId, 5);
      expect(ack.returnCodes, [0x01]);
      expect(ack.properties, isEmpty);
    });
  });

  group('MqttUnsubscribe / MqttUnsuback v5', () {
    test('TC-SUB5-011 v5 UNSUBSCRIBE has property block', () {
      final encoded = MqttUnsubscribe(
        packetId: 5,
        filters: const ['a/b'],
        protocolLevel: 5,
        properties: [
          MqttUserProperty('app', 'mcp_io'),
        ],
      ).encode();
      final parsed = tryParsePacket(encoded)!.packet;
      expect(parsed.packetType, MqttPacketType.unsubscribe);
      // After packetId(2) — property block must be > 0 length byte.
      expect(parsed.body[0], 0x00);
      expect(parsed.body[1], 0x05);
      expect(parsed.body[2], greaterThan(0));
    });

    test('TC-SUB5-012 v5 UNSUBACK fromBodyV5 decodes per-filter codes', () {
      final propBlock = encodeProperties(const []);
      final body = [0x00, 0x05, ...propBlock, 0x00, 0x11]; // success + NoSubscriptionExisted
      final ack = MqttUnsuback.fromBodyV5(body);
      expect(ack.packetId, 5);
      expect(ack.reasonCodes, [0x00, 0x11]);
    });

    test('TC-SUB5-013 v3 UNSUBACK fromBody — empty reasonCodes', () {
      final ack = MqttUnsuback.fromBody(const [0x00, 0x05]);
      expect(ack.packetId, 5);
      expect(ack.reasonCodes, isEmpty);
    });
  });

  group('Adapter wiring — mqtt.subscribe v5 options', () {
    test(
        'TC-SUB5-A-001 NoLocal + RetainAsPublished + RetainHandling forward '
        'into the SUBSCRIBE options byte', () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker',
        clientId: 'c1',
        transport: transport,
        protocolLevel: 5,
      );
      final fut = adapter.connect();
      await Future<void>.delayed(Duration.zero);
      transport.injectIncoming(const MqttIncomingPacket(
        headerByte: MqttPacketType.connack,
        body: [0x00, 0x00, 0x00],
      ));
      await fut;

      final r = await adapter.execute(const Command(
        action: 'mqtt.subscribe', target: 'a/b',
        args: {
          'qos': 1,
          'noLocal': true,
          'retainAsPublished': true,
          'retainHandling': 1, // sendIfNew
        },
      ));
      expect(r.status, CommandStatus.completed);

      // sentPackets[0]=CONNECT, [1]=SUBSCRIBE.
      final sub = transport.parseSent(1);
      expect(sub.packetType, MqttPacketType.subscribe);
      // Last byte of the body is the subscription options byte.
      final optsByte = sub.body.last;
      expect(optsByte & 0x03, 1); // qos
      expect(optsByte & 0x04, 0x04); // NoLocal
      expect(optsByte & 0x08, 0x08); // RetainAsPublished
      expect((optsByte >> 4) & 0x03, 1); // RetainHandling = sendIfNew

      await adapter.disconnect();
    });

    test('TC-SUB5-A-002 args.properties forwards into SUBSCRIBE property block',
        () async {
      final transport = InMemoryMqttTransport();
      final adapter = MqttAdapter(
        deviceId: 'broker',
        clientId: 'c1',
        transport: transport,
        protocolLevel: 5,
      );
      final fut = adapter.connect();
      await Future<void>.delayed(Duration.zero);
      transport.injectIncoming(const MqttIncomingPacket(
        headerByte: MqttPacketType.connack,
        body: [0x00, 0x00, 0x00],
      ));
      await fut;

      await adapter.execute(Command(
        action: 'mqtt.subscribe', target: 'sensors/+',
        args: {
          'qos': 0,
          'properties': [
            MqttVariableIntProperty(
                MqttPropertyId.subscriptionIdentifier, 99),
          ],
        },
      ));

      final sub = transport.parseSent(1);
      // Body: [pidHi, pidLo, propBlockLen, propBlock..., filter..., opts].
      // propBlockLen is at index 2, must be > 0.
      expect(sub.body[2], greaterThan(0));

      await adapter.disconnect();
    });
  });
}

/// MqttAdapter — `AdapterBase` implementation for an MQTT v3.1.1 client.
///
/// The adapter owns the CONNECT / SUBSCRIBE / PUBLISH flow and exposes the
/// 4-Primitive Contract:
///   - `describe`: returns broker identity (client id, connection state).
///   - `read`: not supported for live-pub/sub (MQTT has no synchronous read).
///     Returns a per-target `IoError` so callers can detect it.
///   - `execute`: supports `publish` (QoS 0).
///   - `subscribe`: returns a live `Stream<PayloadEnvelope>` filtered by the
///     topic filter in [TopicSpec.uri].
library;

import 'dart:async';
import 'dart:convert';

import 'package:mcp_bundle/mcp_bundle.dart';
import 'package:mcp_io/mcp_io.dart';

import 'mqtt_codec.dart';
import 'mqtt_transport.dart';

class MqttAdapter extends AdapterBase {
  final String deviceId;
  final String clientId;
  final String? username;
  final String? password;
  final int keepAliveSeconds;
  final MqttTransport _transport;

  IoConnectionState _state = IoConnectionState.disconnected;
  int _nextPacketId = 1;
  StreamSubscription<MqttIncomingPacket>? _incomingSub;
  final StreamController<MqttIncomingPacket> _packetFanout =
      StreamController<MqttIncomingPacket>.broadcast();

  /// Active subscription topic filters, tracked for per-subscriber routing.
  final List<_ActiveSub> _activeSubs = [];

  MqttAdapter({
    required this.deviceId,
    required this.clientId,
    required MqttTransport transport,
    this.username,
    this.password,
    this.keepAliveSeconds = 60,
    AdapterManifest? manifest,
  })  : _transport = transport,
        super(manifest: manifest ?? _defaultManifest);

  static final AdapterManifest _defaultManifest = AdapterManifest(
    adapterId: 'mcp_io_mqtt',
    adapterVersion: '0.1.0',
    contractVersionRange: '>=0.1.0 <1.0.0',
    displayName: 'MQTT Adapter',
    description: 'MQTT v3.1.1 QoS 0 pub/sub adapter.',
  );

  int _allocPacketId() {
    final id = _nextPacketId;
    _nextPacketId = (_nextPacketId + 1) & 0xFFFF;
    if (_nextPacketId == 0) _nextPacketId = 1;
    return id;
  }

  // === Lifecycle ===

  /// Opens the transport, sends CONNECT, awaits CONNACK (return code 0).
  @override
  Future<void> connect() async {
    await _transport.open();
    _incomingSub = _transport.incoming.listen(
      _onIncoming,
      onError: (Object _) {},
      onDone: () {
        _state = IoConnectionState.disconnected;
      },
    );
    final connack = _waitForPacket(MqttPacketType.connack);
    await _transport.send(MqttConnect(
      clientId: clientId,
      username: username,
      password: password,
      keepAliveSeconds: keepAliveSeconds,
    ).encode());
    final ack = await connack.timeout(const Duration(seconds: 5));
    final parsed = MqttConnack.fromBody(ack.body);
    if (parsed.returnCode != 0) {
      throw StateError('CONNECT refused (rc=${parsed.returnCode})');
    }
    _state = IoConnectionState.connected;
  }

  @override
  Future<void> disconnect() async {
    if (_state == IoConnectionState.connected) {
      try {
        await _transport.send(encodeDisconnect());
      } catch (_) {
        // Best effort — the transport may already be broken.
      }
    }
    await _incomingSub?.cancel();
    _incomingSub = null;
    if (!_packetFanout.isClosed) {
      await _packetFanout.close();
    }
    await _transport.close();
    _state = IoConnectionState.disconnected;
  }

  @override
  Future<List<DeviceDescriptor>> probe(dynamic transport) async => const [];

  // === 4-Primitive Contract ===

  @override
  Future<DeviceDescriptor> describe() async {
    return DeviceDescriptor(
      deviceId: deviceId,
      manufacturer: 'MQTT',
      model: 'broker-client',
      transport: 'tcp',
      connectionState: _state,
      serial: clientId,
    );
  }

  /// Synchronous read is not meaningful for MQTT.
  /// Per-target: returns an `exec.unsupported` IoError.
  @override
  Future<ReadResult> read(ReadSpec spec) async {
    final now = DateTime.now();
    return ReadResult(
      items: [
        for (final t in spec.targets)
          ReadResultItem(
            uri: t,
            error: IoError(
              code: 'device.unsupported',
              message: 'MQTT read is async; use subscribe()',
              timestamp: now,
            ),
          ),
      ],
    );
  }

  @override
  Future<CommandResult> execute(Command command) async {
    try {
      switch (command.action) {
        case 'publish': {
          final topic = command.target;
          final payload = _payloadBytes(command.args['payload']);
          final retain = command.args['retain'] == true;
          await _transport.send(MqttPublish(
            topic: topic, payload: payload, qos: 0, retain: retain,
          ).encode());
          return CommandResult(
            status: CommandStatus.completed,
            result: {'topic': topic, 'bytes': payload.length},
          );
        }
        default:
          return CommandResult(
            status: CommandStatus.rejected,
            error: IoError(
              code: 'exec.unknown_action',
              message: 'Unknown action: ${command.action}',
              timestamp: DateTime.now(),
            ),
          );
      }
    } catch (e) {
      return CommandResult(
        status: CommandStatus.failed,
        error: AdapterBase.mapException(e),
      );
    }
  }

  /// Subscribe to a topic filter. Returns a broadcast-style stream of
  /// envelopes that match the filter. SUBSCRIBE is sent on first listen;
  /// UNSUBSCRIBE is sent when the stream is cancelled.
  @override
  Stream<PayloadEnvelope> subscribe(TopicSpec spec) {
    final filter = spec.uri;
    late StreamController<PayloadEnvelope> ctrl;
    _ActiveSub? active;
    ctrl = StreamController<PayloadEnvelope>.broadcast(
      onListen: () async {
        final packetId = _allocPacketId();
        active = _ActiveSub(
          filter: filter, packetId: packetId, controller: ctrl,
        );
        _activeSubs.add(active!);
        await _transport.send(MqttSubscribe(
          packetId: packetId,
          entries: [MqttSubscribeEntry(filter: filter)],
        ).encode());
      },
      onCancel: () async {
        if (active == null) return;
        _activeSubs.remove(active);
        try {
          await _transport.send(MqttUnsubscribe(
            packetId: _allocPacketId(),
            filters: [filter],
          ).encode());
        } catch (_) {
          // Best effort.
        }
      },
    );
    return ctrl.stream;
  }

  @override
  Future<EmergencyStopResult> emergencyStop(EmergencyStopRequest request) async {
    await disconnect();
    return EmergencyStopResult(success: true, stoppedDevices: [deviceId]);
  }

  // === Internal plumbing ===

  /// Returns a single-shot future that completes when a packet of the given
  /// type arrives.
  Future<MqttIncomingPacket> _waitForPacket(int packetType) async {
    final c = Completer<MqttIncomingPacket>();
    late StreamSubscription<MqttIncomingPacket> sub;
    sub = _packetFanout.stream.listen((p) {
      if (p.packetType == packetType && !c.isCompleted) {
        c.complete(p);
        sub.cancel();
      }
    });
    return c.future;
  }

  void _onIncoming(MqttIncomingPacket packet) {
    if (!_packetFanout.isClosed) _packetFanout.add(packet);
    if (packet.packetType == MqttPacketType.publish) {
      _dispatchPublish(packet);
    }
    // PINGRESP / SUBACK are currently no-ops aside from fanout observers.
  }

  void _dispatchPublish(MqttIncomingPacket packet) {
    final pub = MqttPublish.fromBody(packet.headerByte, packet.body);
    for (final sub in _activeSubs) {
      if (!topicMatches(sub.filter, pub.topic)) continue;
      if (sub.controller.isClosed) continue;
      sub.controller.add(PayloadEnvelope(
        uri: pub.topic,
        kind: PayloadKind.read,
        payload: TypedPayload(
          type: PayloadType.blob,
          value: pub.payload,
          timestamp: DateTime.now(),
        ),
        meta: EnvelopeMeta(
          capturedAt: DateTime.now(),
          sourceAddress: pub.topic,
        ),
      ));
    }
  }

  List<int> _payloadBytes(Object? value) {
    if (value == null) return const [];
    if (value is List<int>) return value;
    if (value is String) return utf8.encode(value);
    return utf8.encode(value.toString());
  }
}

class _ActiveSub {
  final String filter;
  final int packetId;
  final StreamController<PayloadEnvelope> controller;
  _ActiveSub({
    required this.filter,
    required this.packetId,
    required this.controller,
  });
}

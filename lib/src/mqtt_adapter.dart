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

import 'codec/properties.dart';
import 'mqtt_codec.dart';
import 'mqtt_transport.dart';
import 'qos/inflight_tracker.dart';
import 'qos/session_journal.dart';

class MqttAdapter extends AdapterBase {
  final String deviceId;
  final String clientId;
  final String? username;
  final String? password;
  final int keepAliveSeconds;
  final MqttTransport _transport;

  IoConnectionState _state = IoConnectionState.disconnected;
  int _nextPacketId = 1;

  /// Tracker shared between QoS 1/2 publishes — provides
  /// `receiveMaximum` flow control + packet-id reuse semantics.
  final InflightTracker _inflight = InflightTracker();

  /// Journal of unacked QoS 1/2 PUBLISHes. On reconnect with
  /// `sessionPresent = true`, [resumeSession] re-sends every entry
  /// in this journal (PUBLISH+DUP for stage 1, PUBREL for QoS 2
  /// stage 2). Cleared when the broker reports `sessionPresent =
  /// false` or [cleanSession] is `true`.
  final MqttSessionJournal _journal = MqttSessionJournal();

  /// Read-only snapshot of the journal. Exposed for diagnostics
  /// (host process can persist this externally for cross-restart
  /// resume).
  MqttSessionJournal get journal => _journal;

  /// Pending Will configuration applied on the next [connect].
  /// Set via the `mqtt.set_will` capability or constructor.
  String? _willTopic;
  List<int>? _willPayload;
  int _willQos = 0;
  bool _willRetain = false;
  StreamSubscription<MqttIncomingPacket>? _incomingSub;
  final StreamController<MqttIncomingPacket> _packetFanout =
      StreamController<MqttIncomingPacket>.broadcast();

  /// Active subscription topic filters, tracked for per-subscriber routing.
  final List<_ActiveSub> _activeSubs = [];

  /// Pending QoS 1 publishes awaiting PUBACK (key = packetId).
  final Map<int, Completer<void>> _pendingPuback = {};

  /// Pending QoS 2 publishes awaiting PUBREC.
  final Map<int, Completer<void>> _pendingPubrec = {};

  /// Pending QoS 2 PUBRELs awaiting PUBCOMP.
  final Map<int, Completer<void>> _pendingPubcomp = {};

  /// Default response timeout for QoS 1/2 acks. The publish path
  /// retries the broker-bound leg with `DUP=1` once this elapses.
  Duration ackTimeout = const Duration(seconds: 30);

  /// Total attempts (initial + retries) for a QoS 1/2 publish.
  /// Setting to 1 disables retries — failures bubble up as
  /// `CommandStatus.failed` immediately on the first timeout.
  int maxPublishAttempts = 3;

  /// MQTT protocol level for the CONNECT packet — 4 = v3.1.1
  /// (default, BC), 5 = v5. Switching to 5 enables the property
  /// blocks fed via [connectProperties] and [willProperties].
  final int protocolLevel;

  /// `cleanSession` (v3.1.1) / `cleanStart` (v5) — when `false` the
  /// broker is asked to retain server-side session state across
  /// disconnect, and the client preserves its own
  /// [_journal] of unacked QoS 1/2 publishes for re-delivery.
  /// Default `true` matches the historic adapter behaviour.
  final bool cleanSession;

  /// When `true`, [connect] automatically calls [resumeSession] after
  /// CONNACK if the broker reported `sessionPresent = true`. When
  /// `false`, the host process is expected to drive resumption
  /// manually via [resumeSession].
  final bool autoResumeOnReconnect;

  /// CONNECT properties emitted on the next [connect] call when
  /// [protocolLevel] is 5. Common entries:
  ///   - `MqttUint32Property(MqttPropertyId.sessionExpiryInterval, ...)`
  ///   - `MqttUint16Property(MqttPropertyId.receiveMaximum, ...)`
  ///   - `MqttUint32Property(MqttPropertyId.maximumPacketSize, ...)`
  ///   - `MqttUint16Property(MqttPropertyId.topicAliasMaximum, ...)`
  ///   - one or more `MqttUserProperty(...)`
  final List<MqttProperty> connectProperties;

  /// Will properties emitted on the next [connect] call when
  /// [protocolLevel] is 5 and [_willTopic] is non-null. Common
  /// entries: `willDelayInterval`, `payloadFormatIndicator`,
  /// `messageExpiryInterval`, `contentType`, `responseTopic`,
  /// `correlationData`, `userProperty`.
  final List<MqttProperty> willProperties;

  /// Last CONNACK received. v5 properties from the server are
  /// readable here once `connect()` returns.
  MqttConnack? _lastConnack;
  MqttConnack? get lastConnack => _lastConnack;

  MqttAdapter({
    required this.deviceId,
    required this.clientId,
    required MqttTransport transport,
    this.username,
    this.password,
    this.keepAliveSeconds = 60,
    this.protocolLevel = 4,
    this.connectProperties = const [],
    this.willProperties = const [],
    this.cleanSession = true,
    this.autoResumeOnReconnect = true,
    AdapterManifest? manifest,
  })  : _transport = transport,
        super(manifest: manifest ?? _defaultManifest);

  static final AdapterManifest _defaultManifest = AdapterManifest(
    adapterId: 'mcp_io_mqtt',
    adapterVersion: '0.2.0',
    contractVersionRange: '>=0.1.0 <1.0.0',
    displayName: 'MQTT Adapter',
    description:
        'MQTT v3.1.1 + v5 adapter — VBI, control packet types, v5 properties '
        '(27 ids), reason codes, topic filter (+/#), packetId tracker. '
        'QoS 1/2 + Will + persistent + TLS + WebSocket transport deferred to '
        'C-4b~d.',
    capabilities: const [
      CapabilityDescriptor(action: 'mqtt.publish', safetyClass: SafetyClass.guarded),
      CapabilityDescriptor(action: 'mqtt.subscribe', safetyClass: SafetyClass.safe),
      CapabilityDescriptor(action: 'mqtt.unsubscribe', safetyClass: SafetyClass.safe),
      CapabilityDescriptor(action: 'mqtt.set_will', safetyClass: SafetyClass.guarded),
    ],
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
      cleanSession: cleanSession,
      willTopic: _willTopic,
      willPayload: _willPayload,
      willQos: _willQos,
      willRetain: _willRetain,
      protocolLevel: protocolLevel,
      properties: connectProperties,
      willProperties: willProperties,
    ).encode());
    final ack = await connack.timeout(const Duration(seconds: 5));
    final parsed = protocolLevel == 5
        ? MqttConnack.fromBodyV5(ack.body)
        : MqttConnack.fromBody(ack.body);
    _lastConnack = parsed;
    // v5 server may downsize our inflight window via Receive Maximum.
    if (protocolLevel == 5) {
      for (final p in parsed.properties) {
        if (p.id == MqttPropertyId.receiveMaximum &&
            p is MqttUint16Property) {
          // Note: InflightTracker.receiveMaximum is final — host
          // re-creates the adapter when changing the cap. For now
          // we just expose the value via [lastConnack].
        }
      }
    }
    if (parsed.returnCode != 0) {
      throw StateError('CONNECT refused (rc=${parsed.returnCode})');
    }
    _state = IoConnectionState.connected;
    // Persistent-session resume: when the broker reports
    // sessionPresent we may continue any prior in-flight publishes.
    // Otherwise the broker has no record of our session and resending
    // would create duplicates — drop the journal.
    if (cleanSession || !parsed.sessionPresent) {
      _journal.clear();
    } else if (autoResumeOnReconnect) {
      await resumeSession();
    }
  }

  @override
  Future<void> disconnect() async {
    if (_state == IoConnectionState.connected) {
      try {
        await _transport.send(encodeDisconnect(protocolLevel: protocolLevel));
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
    _inflight.clear();
  }

  /// Re-deliver every entry in the journal — PUBLISH+DUP for QoS 1
  /// and QoS 2 stage 1, standalone PUBREL for QoS 2 stage 2. Pending
  /// completers are NOT re-registered here; the application layer is
  /// expected to have already observed the prior failure and is
  /// driving redelivery for its own bookkeeping. Returns the number
  /// of entries replayed.
  ///
  /// Called automatically from [connect] when
  /// [autoResumeOnReconnect] is `true` and the broker reports
  /// `sessionPresent = true`. Hosts that need finer control should
  /// pass `autoResumeOnReconnect: false` and invoke this themselves.
  Future<int> resumeSession() async {
    if (_state != IoConnectionState.connected) {
      throw StateError('resumeSession requires an active connection');
    }
    var sent = 0;
    for (final entry in _journal.entries.toList()) {
      if (entry.stage == MqttPublishStage.awaitingFirstAck) {
        // QoS 1 → PUBLISH+DUP, QoS 2 → PUBLISH+DUP. Either way the
        // broker awaits PUBACK or PUBREC for the same packet id.
        final pub = MqttPublish(
          topic: entry.publish.topic,
          payload: entry.publish.payload,
          qos: entry.publish.qos,
          retain: entry.publish.retain,
          packetId: entry.packetId,
          dup: true, // resume — always DUP=1
          protocolLevel: protocolLevel,
          properties: entry.publish.properties,
        );
        await _transport.send(pub.encode());
      } else {
        // QoS 2 stage 2 — PUBREC was acked but PUBCOMP wasn't.
        // Spec §4.4.2 — resend PUBREL only (no DUP bit on PUBREL).
        await _transport.send(
          encodePubrel(entry.packetId, protocolLevel: protocolLevel),
        );
      }
      sent++;
    }
    return sent;
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
        // Legacy + canonical publish.
        case 'publish':
        case 'mqtt.publish':
          return await _doPublish(command);
        case 'mqtt.subscribe':
          return await _doSubscribePacket(command);
        case 'mqtt.unsubscribe':
          return await _doUnsubscribePacket(command);
        case 'mqtt.set_will':
          return _doSetWill(command);
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

  // === Capability dispatch helpers ===

  Future<CommandResult> _doPublish(Command command) async {
    final topic = command.target;
    if (topic.isEmpty) {
      return _argError('publish requires a non-empty target (topic)');
    }
    final payload = _payloadBytes(command.args['payload']);
    final retain = command.args['retain'] == true;
    final qos = (command.args['qos'] as int?) ?? 0;
    if (qos < 0 || qos > 2) {
      return _argError('publish qos must be 0, 1, or 2');
    }
    // v5 PUBLISH properties — passed through args.properties when
    // the channel is operating in protocolLevel 5. v3.1.1 publishes
    // ignore this regardless.
    final props =
        (command.args['properties'] as List?)?.cast<MqttProperty>() ??
            const <MqttProperty>[];

    // QoS 0 — fire-and-forget.
    if (qos == 0) {
      await _transport.send(MqttPublish(
        topic: topic, payload: payload, qos: 0, retain: retain,
        protocolLevel: protocolLevel,
        properties: props,
      ).encode());
      return CommandResult(
        status: CommandStatus.completed,
        result: {'topic': topic, 'bytes': payload.length, 'qos': 0},
      );
    }

    // QoS 1 / 2 — reserve a packet id from the inflight tracker (which
    // also enforces `receiveMaximum` flow control), then run the
    // publish flow with retransmission on ack timeout.
    final packetId = await _inflight.reserve();
    var attempts = 0;
    try {
      if (qos == 1) {
        await _publishQos1(packetId, topic, payload, retain,
            properties: props,
            onAttempt: () => attempts++);
        return CommandResult(
          status: CommandStatus.completed,
          result: {
            'topic': topic, 'bytes': payload.length, 'qos': 1,
            'packetId': packetId, 'attempts': attempts,
          },
        );
      }
      await _publishQos2(packetId, topic, payload, retain,
          properties: props,
          onAttempt: () => attempts++);
      return CommandResult(
        status: CommandStatus.completed,
        result: {
          'topic': topic, 'bytes': payload.length, 'qos': 2,
          'packetId': packetId, 'attempts': attempts,
        },
      );
    } finally {
      _inflight.release(packetId);
    }
  }

  /// QoS 1 publish loop with DUP-on-retry. Throws on final timeout —
  /// the caller (`_doPublish`'s try/catch) maps the exception to a
  /// `CommandStatus.failed` result.
  Future<void> _publishQos1(int packetId, String topic, List<int> payload,
      bool retain,
      {required void Function() onAttempt,
      List<MqttProperty> properties = const []}) async {
    // Record the publish in the durable journal — survives transport
    // disconnect so [resumeSession] can resend on reconnect.
    final pub = MqttPublish(
      topic: topic, payload: payload, qos: 1, retain: retain,
      packetId: packetId, dup: false,
      protocolLevel: protocolLevel,
      properties: properties,
    );
    _journal.put(MqttJournalEntry(packetId: packetId, publish: pub));
    for (var attempt = 1; attempt <= maxPublishAttempts; attempt++) {
      onAttempt();
      final ack = Completer<void>();
      _pendingPuback[packetId] = ack;
      try {
        await _transport.send(MqttPublish(
          topic: topic, payload: payload, qos: 1, retain: retain,
          packetId: packetId,
          dup: attempt > 1,
          protocolLevel: protocolLevel,
          properties: properties,
        ).encode());
        await ack.future.timeout(ackTimeout);
        return;
      } on TimeoutException {
        if (attempt == maxPublishAttempts) rethrow;
        // Otherwise loop and resend with DUP=1.
      } finally {
        _pendingPuback.remove(packetId);
      }
    }
  }

  /// QoS 2 publish loop. The PUBLISH→PUBREC leg retries with DUP=1;
  /// the PUBREL→PUBCOMP leg retries the same PUBREL (no DUP flag —
  /// PUBREL has no DUP bit per the spec).
  Future<void> _publishQos2(int packetId, String topic, List<int> payload,
      bool retain,
      {required void Function() onAttempt,
      List<MqttProperty> properties = const []}) async {
    // Journal the publish before stage 1 so a transport drop here is
    // recoverable via [resumeSession].
    final pub = MqttPublish(
      topic: topic, payload: payload, qos: 2, retain: retain,
      packetId: packetId, dup: false,
      protocolLevel: protocolLevel,
      properties: properties,
    );
    _journal.put(MqttJournalEntry(packetId: packetId, publish: pub));
    // 1) PUBLISH → PUBREC
    for (var attempt = 1; attempt <= maxPublishAttempts; attempt++) {
      onAttempt();
      final pubrec = Completer<void>();
      _pendingPubrec[packetId] = pubrec;
      try {
        await _transport.send(MqttPublish(
          topic: topic, payload: payload, qos: 2, retain: retain,
          packetId: packetId,
          dup: attempt > 1,
          protocolLevel: protocolLevel,
          properties: properties,
        ).encode());
        await pubrec.future.timeout(ackTimeout);
        break;
      } on TimeoutException {
        if (attempt == maxPublishAttempts) rethrow;
      } finally {
        _pendingPubrec.remove(packetId);
      }
    }
    // 2) PUBREL → PUBCOMP
    for (var attempt = 1; attempt <= maxPublishAttempts; attempt++) {
      final pubcomp = Completer<void>();
      _pendingPubcomp[packetId] = pubcomp;
      try {
        await _transport.send(
          encodePubrel(packetId, protocolLevel: protocolLevel),
        );
        await pubcomp.future.timeout(ackTimeout);
        return;
      } on TimeoutException {
        if (attempt == maxPublishAttempts) rethrow;
      } finally {
        _pendingPubcomp.remove(packetId);
      }
    }
  }

  /// Sends a SUBSCRIBE packet for the supplied filter without registering
  /// a stream consumer. Useful for shared-subscription setup or when the
  /// caller wants to drive the receive loop manually. To actually consume
  /// matching messages, use the [subscribe] primitive.
  Future<CommandResult> _doSubscribePacket(Command command) async {
    final filter = (command.args['filter'] as String?) ?? command.target;
    if (filter.isEmpty) {
      return _argError('mqtt.subscribe requires args["filter"] or target');
    }
    final qos = (command.args['qos'] as int?) ?? 0;
    final noLocal = command.args['noLocal'] == true;
    final retainAsPublished = command.args['retainAsPublished'] == true;
    final retainHandling = MqttRetainHandling.fromId(
      (command.args['retainHandling'] as int?) ?? 0,
    );
    final props =
        (command.args['properties'] as List?)?.cast<MqttProperty>() ??
            const <MqttProperty>[];
    final packetId = _allocPacketId();
    await _transport.send(MqttSubscribe(
      packetId: packetId,
      entries: [
        MqttSubscribeEntry(
          filter: filter,
          qos: qos,
          noLocal: noLocal,
          retainAsPublished: retainAsPublished,
          retainHandling: retainHandling,
        ),
      ],
      protocolLevel: protocolLevel,
      properties: props,
    ).encode());
    return CommandResult(
      status: CommandStatus.completed,
      result: {'filter': filter, 'qos': qos, 'packetId': packetId},
    );
  }

  Future<CommandResult> _doUnsubscribePacket(Command command) async {
    final filter = (command.args['filter'] as String?) ?? command.target;
    if (filter.isEmpty) {
      return _argError('mqtt.unsubscribe requires args["filter"] or target');
    }
    final props =
        (command.args['properties'] as List?)?.cast<MqttProperty>() ??
            const <MqttProperty>[];
    final packetId = _allocPacketId();
    await _transport.send(MqttUnsubscribe(
      packetId: packetId, filters: [filter],
      protocolLevel: protocolLevel,
      properties: props,
    ).encode());
    return CommandResult(
      status: CommandStatus.completed,
      result: {'filter': filter, 'packetId': packetId},
    );
  }

  /// Stores Will configuration to be applied on the next [connect]. The
  /// MQTT protocol carries the Will inside CONNECT, so calling
  /// `mqtt.set_will` while already connected is rejected — disconnect
  /// and reconnect to take effect.
  CommandResult _doSetWill(Command command) {
    if (_state == IoConnectionState.connected) {
      return _argError(
        'mqtt.set_will rejected: already connected — Will is bound to CONNECT',
      );
    }
    final topic = command.args['topic'] as String?;
    if (topic == null || topic.isEmpty) {
      return _argError('mqtt.set_will requires args["topic"]');
    }
    _willTopic = topic;
    _willPayload = _payloadBytes(command.args['payload']);
    _willQos = (command.args['qos'] as int?) ?? 0;
    _willRetain = command.args['retain'] == true;
    return CommandResult(
      status: CommandStatus.completed,
      result: {
        'topic': topic,
        'qos': _willQos,
        'retain': _willRetain,
        'bytes': _willPayload!.length,
      },
    );
  }

  CommandResult _argError(String reason) => CommandResult(
        status: CommandStatus.rejected,
        error: IoError(
          code: 'exec.invalid_args',
          message: reason,
          timestamp: DateTime.now(),
        ),
      );

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
          protocolLevel: protocolLevel,
        ).encode());
      },
      onCancel: () async {
        if (active == null) return;
        _activeSubs.remove(active);
        try {
          await _transport.send(MqttUnsubscribe(
            packetId: _allocPacketId(),
            filters: [filter],
            protocolLevel: protocolLevel,
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
    switch (packet.packetType) {
      case MqttPacketType.publish:
        _dispatchPublish(packet);
        // QoS 1/2 inbound — acknowledge to the broker.
        _ackInboundPublish(packet);
        break;
      case MqttPacketType.puback:
        {
          final id = decodeAckPacketId(packet.body);
          _completeAck(_pendingPuback, id);
          if (id >= 0) _journal.release(id);
        }
        break;
      case MqttPacketType.pubrec:
        {
          final id = decodeAckPacketId(packet.body);
          _completeAck(_pendingPubrec, id);
          if (id >= 0) _journal.markPubrecReceived(id);
        }
        break;
      case MqttPacketType.pubcomp:
        {
          final id = decodeAckPacketId(packet.body);
          _completeAck(_pendingPubcomp, id);
          if (id >= 0) _journal.release(id);
        }
        break;
      case MqttPacketType.pubrel:
        // Inbound PUBREL — broker has finished its half of QoS 2.
        // Reply with PUBCOMP. The packet id is in the body.
        final id = decodeAckPacketId(packet.body);
        if (id >= 0) {
          // Best-effort send; failures bubble up via the transport.
          _transport.send(
            encodePubcomp(id, protocolLevel: protocolLevel),
          );
        }
        break;
      default:
        // PINGRESP / SUBACK / UNSUBACK are visible via the fanout
        // stream; no inline handling needed.
        break;
    }
  }

  void _completeAck(Map<int, Completer<void>> table, int packetId) {
    if (packetId < 0) return;
    final c = table[packetId];
    if (c != null && !c.isCompleted) c.complete();
  }

  void _ackInboundPublish(MqttIncomingPacket packet) {
    final pub = MqttPublish.fromBody(packet.headerByte, packet.body);
    final id = pub.packetId;
    if (id == null) return;
    if (pub.qos == 1) {
      _transport.send(encodePuback(id, protocolLevel: protocolLevel));
    } else if (pub.qos == 2) {
      // First leg of QoS 2 inbound — broker waits for PUBREC.
      _transport.send(encodePubrec(id, protocolLevel: protocolLevel));
      // Second leg (PUBREL → PUBCOMP) is handled when PUBREL arrives.
    }
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

/// MQTT transport abstraction (byte-level I/O + incoming packet stream).
library;

import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'mqtt_codec.dart';

abstract class MqttTransport {
  /// Open the underlying connection (idempotent).
  Future<void> open();

  /// Send a fully-encoded MQTT control packet.
  Future<void> send(Uint8List packet);

  /// Stream of incoming packets, framed by the codec.
  Stream<MqttIncomingPacket> get incoming;

  /// Close the underlying connection.
  Future<void> close();
}

/// Production transport using dart:io TCP socket (MQTT over plain TCP).
class TcpMqttTransport implements MqttTransport {
  final String host;
  final int port;
  final Duration connectTimeout;

  Socket? _socket;
  final List<int> _buffer = [];
  final StreamController<MqttIncomingPacket> _incomingCtrl =
      StreamController<MqttIncomingPacket>.broadcast();

  TcpMqttTransport({
    required this.host,
    required this.port,
    this.connectTimeout = const Duration(seconds: 3),
  });

  @override
  Future<void> open() async {
    if (_socket != null) return;
    _socket = await Socket.connect(host, port, timeout: connectTimeout);
    _socket!.listen(
      _onData,
      onError: (e) {
        _incomingCtrl.addError(e);
      },
      onDone: () {
        if (!_incomingCtrl.isClosed) {
          _incomingCtrl.close();
        }
      },
    );
  }

  void _onData(List<int> data) {
    _buffer.addAll(data);
    while (true) {
      final parsed = tryParsePacket(_buffer);
      if (parsed == null) break;
      _buffer.removeRange(0, parsed.bytesRead);
      _incomingCtrl.add(parsed.packet);
    }
  }

  @override
  Future<void> send(Uint8List packet) async {
    if (_socket == null) {
      throw StateError('transport not open');
    }
    _socket!.add(packet);
  }

  @override
  Stream<MqttIncomingPacket> get incoming => _incomingCtrl.stream;

  @override
  Future<void> close() async {
    await _socket?.close();
    _socket?.destroy();
    _socket = null;
    if (!_incomingCtrl.isClosed) {
      await _incomingCtrl.close();
    }
  }
}

/// In-memory transport for tests. The caller drives incoming packets via
/// [injectIncoming]; sent packets are recorded in [sentPackets] for assertion.
class InMemoryMqttTransport implements MqttTransport {
  final StreamController<MqttIncomingPacket> _incomingCtrl =
      StreamController<MqttIncomingPacket>.broadcast();
  final List<Uint8List> sentPackets = [];
  bool isOpen = false;
  bool isClosed = false;

  @override
  Future<void> open() async {
    isOpen = true;
  }

  @override
  Future<void> send(Uint8List packet) async {
    if (isClosed) {
      throw StateError('transport closed');
    }
    sentPackets.add(Uint8List.fromList(packet));
  }

  @override
  Stream<MqttIncomingPacket> get incoming => _incomingCtrl.stream;

  @override
  Future<void> close() async {
    isClosed = true;
    if (!_incomingCtrl.isClosed) {
      await _incomingCtrl.close();
    }
  }

  /// Simulate an incoming packet from the broker.
  void injectIncoming(MqttIncomingPacket packet) {
    if (_incomingCtrl.isClosed) return;
    _incomingCtrl.add(packet);
  }

  /// Parse a sent packet (by index) for test assertions.
  MqttIncomingPacket parseSent(int index) {
    final raw = sentPackets[index];
    final parsed = tryParsePacket(raw);
    if (parsed == null) {
      throw StateError('sent packet is not fully formed');
    }
    return parsed.packet;
  }
}

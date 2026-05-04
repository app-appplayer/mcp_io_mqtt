import 'dart:async';

/// Issues + tracks unique 16-bit packet identifiers for QoS 1/2 and
/// SUBSCRIBE / UNSUBSCRIBE flows.
///
/// Spec ref: MQTT v5 §2.2.1 (Packet Identifier), §2.3.1.
class PacketIdentifierExhausted implements Exception {
  const PacketIdentifierExhausted();
  @override
  String toString() => 'PacketIdentifierExhausted: all 65535 ids in use';
}

class InflightTracker {
  InflightTracker({this.receiveMaximum = 65535});

  /// Server-side flow control limit (CONNACK `Receive Maximum`).
  /// In-flight count must not exceed this.
  final int receiveMaximum;

  final Map<int, Completer<void>> _waiters = {};
  final Set<int> _used = {};
  int _next = 1;

  int get inflight => _used.length;

  /// Reserve a fresh packet id. Returns the id directly when one is
  /// available; otherwise waits until [release] frees a slot or
  /// throws [PacketIdentifierExhausted] when all ids 1..65535 are
  /// taken.
  Future<int> reserve() async {
    if (_used.length >= 0xFFFF) {
      throw const PacketIdentifierExhausted();
    }
    while (inflight >= receiveMaximum) {
      // Wait for any release.
      final c = Completer<void>();
      _waiters[-1] = c; // sentinel — anyone calling release wakes up
      await c.future;
    }

    var attempts = 0;
    while (_used.contains(_next)) {
      _next = (_next % 0xFFFF) + 1;
      if (++attempts > 0xFFFF) {
        throw const PacketIdentifierExhausted();
      }
    }
    final id = _next;
    _used.add(id);
    _next = (_next % 0xFFFF) + 1;
    return id;
  }

  /// Release a packet id so that future calls to [reserve] may reuse
  /// it. No-op when the id wasn't tracked.
  void release(int packetId) {
    if (_used.remove(packetId)) {
      // Wake any waiting reserve calls.
      final w = _waiters.remove(-1);
      if (w != null && !w.isCompleted) w.complete();
    }
  }

  /// Whether [packetId] is currently in flight.
  bool isInflight(int packetId) => _used.contains(packetId);

  /// Drop all tracking — used on session close.
  void clear() {
    _used.clear();
    final w = _waiters.remove(-1);
    if (w != null && !w.isCompleted) {
      w.completeError(StateError('inflight tracker cleared'));
    }
    _next = 1;
  }
}

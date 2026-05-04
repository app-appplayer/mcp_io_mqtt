/// Journal of in-flight QoS 1/2 PUBLISHes that survive a transport
/// disconnect.
///
/// The MQTT spec lets a non-clean session preserve QoS 1/2 state
/// across reconnects: the publisher is expected to re-deliver any
/// PUBLISH that hadn't been acknowledged yet (with `DUP=1`), and any
/// PUBREL that hadn't received its PUBCOMP yet.
///
/// `MqttSessionJournal` records the minimum needed to perform that
/// re-delivery — the original `MqttPublish` for the QoS 1/2 message,
/// plus a stage marker for QoS 2 to know whether the wire-level
/// resend should be PUBLISH or PUBREL.
library;

import '../mqtt_codec.dart';

/// Wire-level stage of an outstanding QoS 2 publish.
enum MqttPublishStage {
  /// Waiting for PUBREC after the initial PUBLISH (QoS 1 also uses
  /// this stage between PUBLISH and PUBACK).
  awaitingFirstAck,

  /// QoS 2 only — PUBREC arrived; the journal now waits for PUBCOMP
  /// after sending PUBREL.
  awaitingPubcomp,
}

class MqttJournalEntry {
  final int packetId;
  final MqttPublish publish;
  MqttPublishStage stage;

  MqttJournalEntry({
    required this.packetId,
    required this.publish,
    this.stage = MqttPublishStage.awaitingFirstAck,
  });
}

class MqttSessionJournal {
  final Map<int, MqttJournalEntry> _entries = {};

  /// Number of currently-tracked unacked publishes.
  int get unackedCount => _entries.length;

  /// Snapshot of every outstanding entry — exposed for diagnostics
  /// and the resume-on-reconnect drain path.
  Iterable<MqttJournalEntry> get entries => _entries.values;

  /// Add or refresh an entry — called after a PUBLISH is sent, or
  /// when the QoS 2 flow advances past PUBREC.
  void put(MqttJournalEntry entry) {
    _entries[entry.packetId] = entry;
  }

  /// Mark a QoS 2 entry as having received PUBREC (next resend
  /// should be PUBREL, not PUBLISH).
  void markPubrecReceived(int packetId) {
    final e = _entries[packetId];
    if (e != null) e.stage = MqttPublishStage.awaitingPubcomp;
  }

  /// Drop the entry — called on PUBACK (qos 1) or PUBCOMP (qos 2).
  void release(int packetId) {
    _entries.remove(packetId);
  }

  /// Clear the journal — used when the broker reports
  /// `sessionPresent = false`, meaning it has no record of our
  /// session and resending would create duplicates.
  void clear() {
    _entries.clear();
  }

  /// `true` when [packetId] is being tracked.
  bool contains(int packetId) => _entries.containsKey(packetId);
}

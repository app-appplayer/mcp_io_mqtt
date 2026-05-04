/// MQTT topic filter with wildcard support (`+` single level, `#`
/// multi-level).
///
/// Spec ref: MQTT v3.1.1 §4.7 / v5 §4.7. v5 §4.8.2 adds shared
/// subscriptions with the `$share/<ShareName>/<TopicFilter>` prefix —
/// brokers load-balance matching messages across all subscribers in
/// the same group.
class MqttTopicFilter {
  MqttTopicFilter._(this.raw, this._levels, this.shareName);

  /// Parse and validate a filter. Throws [FormatException] on invalid
  /// patterns (e.g. `#` not at end, `++` in same level, `$share/` with
  /// missing group or empty inner filter).
  factory MqttTopicFilter.parse(String filter) {
    if (filter.isEmpty) {
      throw const FormatException('topic filter must not be empty');
    }

    // Detect `$share/<group>/<inner>` per v5 §4.8.2. The inner filter
    // (after the group level) is what is matched against published
    // topic names; the share-name is metadata for load balancing.
    String? shareName;
    var working = filter;
    if (working.startsWith(r'$share/')) {
      final body = working.substring(r'$share/'.length);
      final slash = body.indexOf('/');
      if (slash <= 0 || slash == body.length - 1) {
        throw FormatException(
            'shared subscription "$filter": expected '
            r'`$share/<group>/<topicFilter>`');
      }
      shareName = body.substring(0, slash);
      if (shareName.contains('+') || shareName.contains('#')) {
        throw FormatException(
            'shared subscription "$filter": share name must not contain '
            'wildcards');
      }
      working = body.substring(slash + 1);
      if (working.isEmpty) {
        throw FormatException(
            'shared subscription "$filter": inner topic filter is empty');
      }
    }

    final levels = working.split('/');
    for (var i = 0; i < levels.length; i++) {
      final level = levels[i];
      if (level == '+') continue;
      if (level == '#') {
        if (i != levels.length - 1) {
          throw FormatException(
              'topic filter "$filter": `#` must be the last level');
        }
        continue;
      }
      if (level.contains('+') || level.contains('#')) {
        throw FormatException(
            'topic filter "$filter": wildcards must occupy an entire level');
      }
    }
    return MqttTopicFilter._(filter, levels, shareName);
  }

  /// Original filter string (including `$share/<group>/` prefix when
  /// present).
  final String raw;
  final List<String> _levels;

  /// Share-name when this is a shared subscription
  /// (`$share/<shareName>/<topicFilter>`); `null` for normal filters.
  final String? shareName;

  /// True when this is an MQTT v5 shared subscription.
  bool get isShared => shareName != null;

  /// True when [topic] matches this filter under MQTT wildcard rules.
  bool matches(String topic) {
    final tlevels = topic.split('/');
    return _matchLevels(_levels, tlevels);
  }

  static bool _matchLevels(List<String> filter, List<String> topic) {
    var fi = 0;
    var ti = 0;
    while (fi < filter.length && ti < topic.length) {
      final f = filter[fi];
      if (f == '#') return true;
      if (f != '+' && f != topic[ti]) return false;
      fi++;
      ti++;
    }
    // Edge: filter ends with `/#` should match zero further levels.
    if (fi == filter.length - 1 && filter[fi] == '#') return true;
    if (fi != filter.length || ti != topic.length) return false;
    return true;
  }

  @override
  bool operator ==(Object other) =>
      other is MqttTopicFilter && other.raw == raw;

  @override
  int get hashCode => raw.hashCode;

  @override
  String toString() => raw;
}

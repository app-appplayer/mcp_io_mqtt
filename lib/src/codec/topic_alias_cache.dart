/// Topic Alias cache for MQTT v5 (`§3.3.2.3.4 Topic Alias`).
///
/// MQTT v5 lets the broker advertise a `topicAliasMaximum` so the
/// client can compress repeated PUBLISH topic names: the first
/// publish on a topic carries both the topic name and a freshly
/// allocated 16-bit alias; later publishes on the same topic send an
/// empty topic name plus the alias only. This cache implements the
/// client-side bookkeeping with a simple LRU eviction policy bounded
/// by the broker-advertised capacity.
///
/// Thread-safety: per-session, accessed from the codec/transport
/// thread only. No locking.
library;

class TopicAliasCache {
  /// Maximum number of distinct aliases the broker is willing to
  /// accept (the value of `topicAliasMaximum` from CONNACK). Aliases
  /// are 1..[capacity] inclusive — `0` is reserved per spec
  /// §3.3.2.3.4.
  ///
  /// When the broker advertises `0` (or omits the property), aliasing
  /// is disabled and [aliasFor] / [assignAlias] return `null`.
  final int capacity;

  /// `topic → alias` lookup.
  final Map<String, int> _topicToAlias = <String, int>{};

  /// LRU list of topics, oldest at the front.
  final List<String> _lru = <String>[];

  /// Set of aliases currently in use; lets `assignAlias` skip
  /// reserved values quickly.
  final Set<int> _usedAliases = <int>{};

  TopicAliasCache({required this.capacity}) {
    if (capacity < 0) {
      throw ArgumentError.value(capacity, 'capacity', 'must be non-negative');
    }
  }

  /// `true` when topic-aliasing is disabled (broker advertised 0).
  bool get isDisabled => capacity == 0;

  /// Return the cached alias for [topic], or `null` if not yet
  /// assigned. Touches the LRU position.
  int? aliasFor(String topic) {
    if (isDisabled) return null;
    final alias = _topicToAlias[topic];
    if (alias == null) return null;
    _touch(topic);
    return alias;
  }

  /// Allocate a new alias for [topic]. If an alias is already
  /// assigned, returns the existing one and refreshes its LRU
  /// position. If the cache is full, evicts the least-recently-used
  /// entry and reuses its alias slot.
  ///
  /// Returns `null` when [capacity] is `0` (aliasing disabled).
  int? assignAlias(String topic) {
    if (isDisabled) return null;
    final existing = _topicToAlias[topic];
    if (existing != null) {
      _touch(topic);
      return existing;
    }

    int alias;
    if (_topicToAlias.length < capacity) {
      // Find the lowest unused alias in 1..capacity.
      alias = 1;
      while (_usedAliases.contains(alias)) {
        alias++;
      }
    } else {
      // Evict the LRU entry and reuse its slot.
      final evicted = _lru.removeAt(0);
      alias = _topicToAlias.remove(evicted)!;
      // Note: `evicted` no longer maps; alias slot stays in
      // `_usedAliases` because we're about to reassign it below.
    }

    _topicToAlias[topic] = alias;
    _usedAliases.add(alias);
    _lru.add(topic);
    return alias;
  }

  /// Drop all entries (for session reset / re-CONNECT).
  void clear() {
    _topicToAlias.clear();
    _lru.clear();
    _usedAliases.clear();
  }

  /// Number of currently cached topic→alias mappings.
  int get length => _topicToAlias.length;

  /// `true` when [topic] currently has an assigned alias.
  bool contains(String topic) => _topicToAlias.containsKey(topic);

  void _touch(String topic) {
    _lru.remove(topic);
    _lru.add(topic);
  }
}

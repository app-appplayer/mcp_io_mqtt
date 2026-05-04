/// Tests for `TopicAliasCache` — MQTT v5 §3.3.2.3.4 client-side
/// alias bookkeeping with LRU eviction.
@TestOn('vm')
library;

import 'package:mcp_io_mqtt/mcp_io_mqtt.dart';
import 'package:test/test.dart';

void main() {
  group('TopicAliasCache', () {
    test('TC-TA-001 disabled when capacity is 0', () {
      final c = TopicAliasCache(capacity: 0);
      expect(c.isDisabled, isTrue);
      expect(c.aliasFor('a/b'), isNull);
      expect(c.assignAlias('a/b'), isNull);
    });

    test('TC-TA-002 first assign returns alias 1, lookup hits', () {
      final c = TopicAliasCache(capacity: 4);
      final a = c.assignAlias('plant/temp');
      expect(a, 1);
      expect(c.aliasFor('plant/temp'), 1);
      expect(c.length, 1);
      expect(c.contains('plant/temp'), isTrue);
    });

    test('TC-TA-003 distinct topics get distinct aliases (1..capacity)',
        () {
      final c = TopicAliasCache(capacity: 4);
      final a1 = c.assignAlias('a');
      final a2 = c.assignAlias('b');
      final a3 = c.assignAlias('c');
      expect({a1, a2, a3}, {1, 2, 3});
      expect(c.length, 3);
    });

    test('TC-TA-004 re-assigning same topic returns same alias + touches LRU',
        () {
      final c = TopicAliasCache(capacity: 2);
      final a1 = c.assignAlias('a');
      final a2 = c.assignAlias('b');
      // Re-assign 'a' → same alias, also makes 'a' MRU.
      expect(c.assignAlias('a'), a1);

      // Now adding 'c' should evict the LRU entry which is 'b'
      // (because 'a' was just touched).
      final a3 = c.assignAlias('c');
      expect(a3, a2,
          reason: 'evicted slot from b (LRU) should be reused for c');
      expect(c.contains('a'), isTrue);
      expect(c.contains('b'), isFalse);
      expect(c.contains('c'), isTrue);
    });

    test('TC-TA-005 aliasFor on miss returns null', () {
      final c = TopicAliasCache(capacity: 4);
      c.assignAlias('a');
      expect(c.aliasFor('b'), isNull);
    });

    test('TC-TA-006 aliasFor refreshes LRU position', () {
      final c = TopicAliasCache(capacity: 2);
      final a = c.assignAlias('a');
      c.assignAlias('b'); // 'a' becomes LRU
      // Touch 'a' via aliasFor.
      expect(c.aliasFor('a'), a);
      // Now adding 'c' should evict 'b' (LRU again after the touch).
      c.assignAlias('c');
      expect(c.contains('a'), isTrue);
      expect(c.contains('b'), isFalse);
      expect(c.contains('c'), isTrue);
    });

    test('TC-TA-007 clear empties the cache', () {
      final c = TopicAliasCache(capacity: 2);
      c.assignAlias('a');
      c.assignAlias('b');
      c.clear();
      expect(c.length, 0);
      expect(c.contains('a'), isFalse);
      expect(c.aliasFor('a'), isNull);
    });

    test('TC-TA-008 negative capacity rejected', () {
      expect(() => TopicAliasCache(capacity: -1), throwsArgumentError);
    });
  });
}

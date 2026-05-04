import 'package:mcp_io_mqtt/mcp_io_mqtt.dart';
import 'package:test/test.dart';

void main() {
  group('InflightTracker', () {
    test('TC-INF-001 reserve returns sequential ids', () async {
      final t = InflightTracker();
      expect(await t.reserve(), 1);
      expect(await t.reserve(), 2);
      expect(await t.reserve(), 3);
    });

    test('TC-INF-002 release frees id for reuse', () async {
      final t = InflightTracker();
      final a = await t.reserve();
      final b = await t.reserve();
      t.release(a);
      // Reservation may pick a (now free) or wrap around — but must be unique.
      final c = await t.reserve();
      expect(c, isNot(equals(b)));
      expect(t.isInflight(c), isTrue);
      expect(t.isInflight(a), c == a ? isTrue : isFalse);
    });

    test('TC-INF-003 isInflight tracking', () async {
      final t = InflightTracker();
      final id = await t.reserve();
      expect(t.isInflight(id), isTrue);
      t.release(id);
      expect(t.isInflight(id), isFalse);
    });

    test('TC-INF-004 receive maximum throttles via wait', () async {
      final t = InflightTracker(receiveMaximum: 2);
      final a = await t.reserve();
      await t.reserve();
      // 3rd should pend until a release.
      var fired = false;
      final third = t.reserve().then((id) => fired = true);
      await Future<void>.delayed(const Duration(milliseconds: 10));
      expect(fired, isFalse);
      t.release(a);
      await third;
      expect(fired, isTrue);
    });

    test('TC-INF-005 clear empties tracker', () async {
      final t = InflightTracker();
      await t.reserve();
      await t.reserve();
      t.clear();
      expect(t.inflight, 0);
    });
  });
}

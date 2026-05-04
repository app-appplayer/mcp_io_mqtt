import 'package:mcp_io_mqtt/mcp_io_mqtt.dart';
import 'package:test/test.dart';

void main() {
  group('MqttTopicFilter - exact', () {
    test('TC-FLT-001 exact match', () {
      final f = MqttTopicFilter.parse('a/b/c');
      expect(f.matches('a/b/c'), isTrue);
      expect(f.matches('a/b'), isFalse);
      expect(f.matches('a/b/c/d'), isFalse);
    });
  });

  group('MqttTopicFilter - + (single level)', () {
    test('TC-FLT-010 single level wildcard', () {
      final f = MqttTopicFilter.parse('plant/+/temperature');
      expect(f.matches('plant/zone1/temperature'), isTrue);
      expect(f.matches('plant/zone2/temperature'), isTrue);
      expect(f.matches('plant/zone1/sub/temperature'), isFalse);
    });

    test('TC-FLT-011 leading + matches anything one-level', () {
      final f = MqttTopicFilter.parse('+/status');
      expect(f.matches('a/status'), isTrue);
      expect(f.matches('b/status'), isTrue);
      expect(f.matches('a/b/status'), isFalse);
    });

    test('TC-FLT-012 multiple +', () {
      final f = MqttTopicFilter.parse('+/+/temp');
      expect(f.matches('a/b/temp'), isTrue);
      expect(f.matches('a/temp'), isFalse);
    });
  });

  group('MqttTopicFilter - # (multi-level)', () {
    test('TC-FLT-020 # at end matches deeper', () {
      final f = MqttTopicFilter.parse('plant/#');
      expect(f.matches('plant/zone1'), isTrue);
      expect(f.matches('plant/zone1/sub/temp'), isTrue);
    });

    test('TC-FLT-021 # only matches root', () {
      final f = MqttTopicFilter.parse('#');
      expect(f.matches('anything'), isTrue);
      expect(f.matches('a/b/c'), isTrue);
    });

    test('TC-FLT-022 # not at end is invalid', () {
      expect(() => MqttTopicFilter.parse('a/#/b'), throwsFormatException);
    });
  });

  group('MqttTopicFilter - invalid', () {
    test('TC-FLT-030 wildcard inside level', () {
      expect(() => MqttTopicFilter.parse('a+/b'), throwsFormatException);
      expect(() => MqttTopicFilter.parse('a/#b'), throwsFormatException);
    });

    test('TC-FLT-031 empty filter', () {
      expect(() => MqttTopicFilter.parse(''), throwsFormatException);
    });
  });

  group('MqttTopicFilter - equality', () {
    test('TC-FLT-040 raw equality', () {
      expect(MqttTopicFilter.parse('a/+/b'),
          equals(MqttTopicFilter.parse('a/+/b')));
    });
  });

  group(r'MqttTopicFilter - $share/<group>/ shared subscription (v5)', () {
    test('TC-FLT-050 shared subscription parse + isShared', () {
      final f = MqttTopicFilter.parse(r'$share/group1/plant/+/temp');
      expect(f.isShared, isTrue);
      expect(f.shareName, 'group1');
      // Inner filter must still match against published topic names.
      expect(f.matches('plant/zone1/temp'), isTrue);
      expect(f.matches('plant/zone2/temp'), isTrue);
      expect(f.matches('plant/zone1/foo'), isFalse);
    });

    test('TC-FLT-051 normal filter has no shareName', () {
      final f = MqttTopicFilter.parse('plant/+/temp');
      expect(f.isShared, isFalse);
      expect(f.shareName, isNull);
    });

    test('TC-FLT-052 missing group rejected', () {
      expect(() => MqttTopicFilter.parse(r'$share//topic'),
          throwsFormatException);
    });

    test('TC-FLT-053 missing inner filter rejected', () {
      expect(() => MqttTopicFilter.parse(r'$share/group1/'),
          throwsFormatException);
    });

    test('TC-FLT-054 wildcards in share-name rejected', () {
      expect(() => MqttTopicFilter.parse(r'$share/+/topic'),
          throwsFormatException);
      expect(() => MqttTopicFilter.parse(r'$share/#/topic'),
          throwsFormatException);
    });

    test(r'TC-FLT-055 only $share/<name> without inner filter rejected',
        () {
      expect(() => MqttTopicFilter.parse(r'$share/onlygroup'),
          throwsFormatException);
    });
  });
}

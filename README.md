# mcp_io_mqtt

MQTT adapter for [`mcp_io`](https://pub.dev/packages/mcp_io) — talk
to IoT brokers (Mosquitto, HiveMQ, EMQX, AWS IoT, Azure IoT, …)
through the same 4-Primitive surface used by the rest of the
adapter family.

## Capability matrix

| Area | Support |
|---|---|
| Protocol | v3.1.1 (level 4) — Will, retain, persistent session, wildcards `+`/`#` |
| v5 codec | Variable-byte integer, control packet types, 27 properties, reason codes (consumer of these decodes is wired into `MqttCodec` for callers) |
| Transport | Pluggable `MqttTransport`; TCP plain / TLS / WebSocket (host project supplies the byte transport) |
| QoS | 0 today; 1 / 2 PacketId tracker (`InflightTracker` + `receiveMaximum` reservation) shipped, full sequence wiring in progress |
| Topic filter | `+`, `#` with full validation (last-segment-only `#`, no embedded wildcards) |
| Reason codes | All v5 reason code → `IoError` mapping (`mapping/reason_to_ioerror.dart`) |
| Capabilities | `mqtt.publish`, `mqtt.subscribe`, `mqtt.unsubscribe`, `mqtt.set_will` |

## Quick start

```dart
import 'package:mcp_io_mqtt/mcp_io_mqtt.dart';

final adapter = MqttAdapter(
  deviceId: 'broker-1',
  clientId: 'my-client',
  transport: InMemoryMqttTransport(), // production: TCP / TLS / WS
);
await adapter.connect();

// Capability-id flow (0.2.0+).
await adapter.execute(const Command(
  action: 'mqtt.publish',
  target: 'sensors/room1/temperature',
  args: {'payload': '21.5', 'retain': true},
));

final stream = adapter.subscribe(const TopicSpec(uri: 'sensors/+'));
stream.listen((env) => print('${env.uri}: ${env.payload.value}'));
```

## Will (Last Will and Testament)

Set the will *before* `connect()` — the codec emits it inside the
CONNECT packet. Calling `mqtt.set_will` once already connected is
rejected.

```dart
await adapter.execute(const Command(
  action: 'mqtt.set_will',
  target: '',
  args: {
    'topic': 'status/my-client',
    'payload': 'offline',
    'qos': 1,
    'retain': true,
  },
));
await adapter.connect();
```

## License

MIT — see [LICENSE](LICENSE).

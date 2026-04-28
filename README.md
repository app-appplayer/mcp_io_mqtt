# MCP IO MQTT

MQTT v3.1.1 adapter for [`mcp_io`](https://pub.dev/packages/mcp_io). QoS 0 pub/sub with topic wildcard matching.

```dart
import 'package:mcp_io_mqtt/mcp_io_mqtt.dart';

final adapter = MqttIoAdapter(MqttTransport(brokerUri));
registry.register('broker', adapter);
```

## License

MIT — see [LICENSE](LICENSE).

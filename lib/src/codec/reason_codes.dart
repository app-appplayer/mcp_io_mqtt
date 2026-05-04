/// MQTT v5 Reason Codes (selected — Spec §2.4).
///
/// Each ack packet (CONNACK / PUBACK / PUBREC / PUBREL / PUBCOMP /
/// SUBACK / UNSUBACK / DISCONNECT / AUTH) carries a reason code byte.
/// 0x00..0x1F = success / informational, 0x80+ = error.
class MqttReasonCode {
  const MqttReasonCode(this.value, this.name);
  final int value;
  final String name;

  // ----- Success / informational -----
  static const success = MqttReasonCode(0x00, 'Success');
  static const normalDisconnection =
      MqttReasonCode(0x00, 'Normal disconnection');
  static const grantedQos0 = MqttReasonCode(0x00, 'Granted QoS 0');
  static const grantedQos1 = MqttReasonCode(0x01, 'Granted QoS 1');
  static const grantedQos2 = MqttReasonCode(0x02, 'Granted QoS 2');
  static const noMatchingSubscribers =
      MqttReasonCode(0x10, 'No matching subscribers');
  static const noSubscriptionExisted =
      MqttReasonCode(0x11, 'No subscription existed');

  // ----- Client error (0x80~0x9F) -----
  static const unspecifiedError = MqttReasonCode(0x80, 'Unspecified error');
  static const malformedPacket = MqttReasonCode(0x81, 'Malformed Packet');
  static const protocolError = MqttReasonCode(0x82, 'Protocol Error');
  static const implementationSpecificError =
      MqttReasonCode(0x83, 'Implementation specific error');
  static const unsupportedProtocolVersion =
      MqttReasonCode(0x84, 'Unsupported Protocol Version');
  static const clientIdentifierNotValid =
      MqttReasonCode(0x85, 'Client Identifier not valid');
  static const badUserNameOrPassword =
      MqttReasonCode(0x86, 'Bad User Name or Password');
  static const notAuthorized = MqttReasonCode(0x87, 'Not authorized');
  static const serverUnavailable =
      MqttReasonCode(0x88, 'Server unavailable');
  static const serverBusy = MqttReasonCode(0x89, 'Server busy');
  static const banned = MqttReasonCode(0x8A, 'Banned');
  static const serverShuttingDown =
      MqttReasonCode(0x8B, 'Server shutting down');
  static const badAuthenticationMethod =
      MqttReasonCode(0x8C, 'Bad authentication method');
  static const keepAliveTimeout =
      MqttReasonCode(0x8D, 'Keep Alive timeout');
  static const sessionTakenOver =
      MqttReasonCode(0x8E, 'Session taken over');
  static const topicFilterInvalid =
      MqttReasonCode(0x8F, 'Topic Filter invalid');
  static const topicNameInvalid =
      MqttReasonCode(0x90, 'Topic Name invalid');
  static const packetIdentifierInUse =
      MqttReasonCode(0x91, 'Packet Identifier in use');
  static const packetIdentifierNotFound =
      MqttReasonCode(0x92, 'Packet Identifier not found');
  static const receiveMaximumExceeded =
      MqttReasonCode(0x93, 'Receive Maximum exceeded');
  static const topicAliasInvalid =
      MqttReasonCode(0x94, 'Topic Alias invalid');
  static const packetTooLarge = MqttReasonCode(0x95, 'Packet too large');
  static const messageRateTooHigh =
      MqttReasonCode(0x96, 'Message rate too high');
  static const quotaExceeded = MqttReasonCode(0x97, 'Quota exceeded');
  static const administrativeAction =
      MqttReasonCode(0x98, 'Administrative action');
  static const payloadFormatInvalid =
      MqttReasonCode(0x99, 'Payload format invalid');

  // ----- Server error (0xA0+) -----
  static const retainNotSupported =
      MqttReasonCode(0x9A, 'Retain not supported');
  static const qosNotSupported = MqttReasonCode(0x9B, 'QoS not supported');
  static const useAnotherServer = MqttReasonCode(0x9C, 'Use another server');
  static const serverMoved = MqttReasonCode(0x9D, 'Server moved');
  static const sharedSubscriptionsNotSupported =
      MqttReasonCode(0x9E, 'Shared subscriptions not supported');
  static const connectionRateExceeded =
      MqttReasonCode(0x9F, 'Connection rate exceeded');
  static const maximumConnectTime =
      MqttReasonCode(0xA0, 'Maximum connect time');
  static const subscriptionIdentifiersNotSupported = MqttReasonCode(
      0xA1, 'Subscription Identifiers not supported');
  static const wildcardSubscriptionsNotSupported = MqttReasonCode(
      0xA2, 'Wildcard Subscriptions not supported');

  /// True when the reason value is in the success / informational range
  /// (0x00..0x1F). Note that several codes share value 0x00 so callers
  /// must rely on packet context for disambiguation.
  bool get isSuccess => value < 0x80;

  /// True when the reason indicates an error (0x80+).
  bool get isError => value >= 0x80;

  @override
  String toString() => '$name (0x${value.toRadixString(16).padLeft(2, '0')})';
}

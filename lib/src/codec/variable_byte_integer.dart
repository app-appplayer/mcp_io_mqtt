/// MQTT Variable Byte Integer (1–4 bytes).
///
/// Continuation bit (0x80) is set on every byte except the last. The
/// remaining 7 bits per byte carry the value, little-end first.
///
/// Spec ref: MQTT v3.1.1 §2.2.3 / v5 §1.5.5.
class VariableByteInteger {
  VariableByteInteger._();

  /// Maximum encodable value (4-byte form). 268,435,455 = 0xFFFFFFF.
  static const int maxValue = 0x0FFFFFFF;

  /// Encode [value] into 1–4 bytes. Throws [ArgumentError] when out of
  /// range.
  static List<int> encode(int value) {
    if (value < 0 || value > maxValue) {
      throw ArgumentError.value(
          value, 'value', 'must be 0..$maxValue');
    }
    final out = <int>[];
    var v = value;
    do {
      var byte = v & 0x7F;
      v >>= 7;
      if (v > 0) byte |= 0x80;
      out.add(byte);
    } while (v > 0);
    return out;
  }

  /// Decode starting at [offset]. Returns the value plus the byte
  /// length consumed.
  static ({int value, int length}) decode(List<int> bytes, {int offset = 0}) {
    var multiplier = 1;
    var value = 0;
    var consumed = 0;
    while (true) {
      if (offset + consumed >= bytes.length) {
        throw const FormatException('VariableByteInteger: truncated');
      }
      final byte = bytes[offset + consumed];
      consumed++;
      value += (byte & 0x7F) * multiplier;
      if (multiplier > 128 * 128 * 128) {
        throw const FormatException('VariableByteInteger: overflow');
      }
      if ((byte & 0x80) == 0) break;
      multiplier *= 128;
    }
    return (value: value, length: consumed);
  }
}

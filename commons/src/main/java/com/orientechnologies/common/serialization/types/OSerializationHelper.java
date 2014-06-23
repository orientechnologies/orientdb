package com.orientechnologies.common.serialization.types;

import java.util.Map;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSerializationHelper {
  public static final OSerializationHelper INSTANCE = new OSerializationHelper();

  public <K, V> byte[] serialize(Map<K, V> map, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer) {
    final int size = length(map, keySerializer, valueSerializer);
    byte[] stream = new byte[size];

    serialize(map, stream, 0, keySerializer, valueSerializer);

    return stream;
  }

  private <K, V> void serialize(Map<K, V> map, byte[] stream, int offset, OBinarySerializer<K> keySerializer,
      OBinarySerializer<V> valueSerializer) {
    OIntegerSerializer.INSTANCE.serialize(map.size(), stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (Map.Entry<K, V> entry : map.entrySet()) {
      keySerializer.serialize(entry.getKey(), stream, offset);
      offset += keySerializer.getObjectSize(entry.getKey());

      valueSerializer.serialize(entry.getValue(), stream, offset);
      offset += valueSerializer.getObjectSize(entry.getValue());
    }
  }

  public <K, V> int length(Map<K, V> map, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer) {
    int mapSize = map.size();
    int length = OIntegerSerializer.INT_SIZE;
    assert keySerializer.isFixedLength();
    length += mapSize * keySerializer.getFixedLength();

    assert valueSerializer.isFixedLength();
    length += mapSize * valueSerializer.getFixedLength();

    return length;
  }
}

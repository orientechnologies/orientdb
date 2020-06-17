/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.common.serialization.types;

import java.util.Map;

/** @author Artem Orobets (enisher-at-gmail.com) */
public class OSerializationHelper {
  public static final OSerializationHelper INSTANCE = new OSerializationHelper();

  public <K, V> byte[] serialize(
      Map<K, V> map, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer) {
    final int size = length(map, keySerializer, valueSerializer);
    final byte[] stream = new byte[size];

    serialize(map, stream, 0, keySerializer, valueSerializer);

    return stream;
  }

  public <K, V> int length(
      Map<K, V> map, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer) {
    final int mapSize = map.size();
    int length = OIntegerSerializer.INT_SIZE;
    assert keySerializer.isFixedLength();
    length += mapSize * keySerializer.getFixedLength();

    assert valueSerializer.isFixedLength();
    length += mapSize * valueSerializer.getFixedLength();

    return length;
  }

  private <K, V> void serialize(
      Map<K, V> map,
      byte[] stream,
      int offset,
      OBinarySerializer<K> keySerializer,
      OBinarySerializer<V> valueSerializer) {
    OIntegerSerializer.INSTANCE.serializeLiteral(map.size(), stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (Map.Entry<K, V> entry : map.entrySet()) {
      keySerializer.serialize(entry.getKey(), stream, offset);
      offset += keySerializer.getObjectSize(entry.getKey());

      valueSerializer.serialize(entry.getValue(), stream, offset);
      offset += valueSerializer.getObjectSize(entry.getValue());
    }
  }
}

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
package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import com.orientechnologies.orient.client.remote.TreeEntry;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataInput;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelDataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class OSBTFetchEntriesMajorResponse<K, V> implements OBinaryResponse {
  private final OBinarySerializer<K> keySerializer;
  private final OBinarySerializer<V> valueSerializer;
  private List<Map.Entry<K, V>> list;

  public OSBTFetchEntriesMajorResponse(
      OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer) {
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  public OSBTFetchEntriesMajorResponse(
      OBinarySerializer<K> keySerializer,
      OBinarySerializer<V> valueSerializer,
      List<Map.Entry<K, V>> list) {
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.list = list;
  }

  @Override
  public void read(OChannelDataInput network, OStorageRemoteSession session) throws IOException {
    byte[] stream = network.readBytes();
    int offset = 0;
    final int count = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, 0);
    offset += OIntegerSerializer.INT_SIZE;
    list = new ArrayList<Map.Entry<K, V>>(count);
    for (int i = 0; i < count; i++) {
      final K resultKey = keySerializer.deserialize(stream, offset);
      offset += keySerializer.getObjectSize(stream, offset);
      final V resultValue = valueSerializer.deserialize(stream, offset);
      offset += valueSerializer.getObjectSize(stream, offset);
      list.add(new TreeEntry<K, V>(resultKey, resultValue));
    }
  }

  public void write(OChannelDataOutput channel, int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    byte[] stream =
        new byte
            [OIntegerSerializer.INT_SIZE
                + list.size()
                    * (keySerializer.getFixedLength() + valueSerializer.getFixedLength())];
    int offset = 0;

    OIntegerSerializer.INSTANCE.serializeLiteral(list.size(), stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (Entry<K, V> entry : list) {
      keySerializer.serialize(entry.getKey(), stream, offset);
      offset += keySerializer.getObjectSize(entry.getKey());

      valueSerializer.serialize(entry.getValue(), stream, offset);
      offset += valueSerializer.getObjectSize(entry.getValue());
    }

    channel.writeBytes(stream);
  }

  public List<Map.Entry<K, V>> getList() {
    return list;
  }
}

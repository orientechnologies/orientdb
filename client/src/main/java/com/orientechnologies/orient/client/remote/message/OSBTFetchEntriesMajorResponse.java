package com.orientechnologies.orient.client.remote.message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.client.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OSBTreeBonsaiRemote;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;

public class OSBTFetchEntriesMajorResponse<K, V> implements OBinaryResponse<List<Entry<K, V>>> {
  private final OBinarySerializer<K> keySerializer;
  private final OBinarySerializer<V> valueSerializer;

  public OSBTFetchEntriesMajorResponse(OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer) {
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  @Override
  public List<Entry<K, V>> read(OChannelBinaryAsynchClient network, OStorageRemoteSession session) throws IOException {
    List<Map.Entry<K, V>> list = null;
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
      list.add(new OSBTreeBonsaiRemote.TreeEntry<K, V>(resultKey, resultValue));
    }
    return list;
  }
}
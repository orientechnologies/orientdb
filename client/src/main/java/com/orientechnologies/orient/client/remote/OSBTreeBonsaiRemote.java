/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.client.remote;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeRidBag;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link OSBTreeBonsai} for remote storage.
 * 
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class OSBTreeBonsaiRemote<K, V> implements OSBTreeBonsai<K, V> {
  private final OBonsaiCollectionPointer treePointer;
  private final OBinarySerializer<K>     keySerializer;
  private final OBinarySerializer<V>     valueSerializer;

  public OSBTreeBonsaiRemote(OBonsaiCollectionPointer treePointer, OBinarySerializer<K> keySerializer,
      OBinarySerializer<V> valueSerializer) {
    this.treePointer = treePointer;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  @Override
  public long getFileId() {
    return treePointer.getFileId();
  }

  @Override
  public OBonsaiBucketPointer getRootBucketPointer() {
    return treePointer.getRootPointer();
  }

  @Override
  public OBonsaiCollectionPointer getCollectionPointer() {
    return treePointer;
  }

  @Override
  public V get(K key) {
    final OStorageRemote storage = (OStorageRemote) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying();

    final byte[] keyStream = new byte[keySerializer.getObjectSize(key)];
    keySerializer.serialize(key, keyStream, 0);
    return storage.networkOperation(new OStorageRemoteOperation<V>() {
      @Override
      public V execute(final OChannelBinaryAsynchClient client) throws IOException {
        storage.beginRequest(client,OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET);
        OCollectionNetworkSerializer.INSTANCE.writeCollectionPointer(client, getCollectionPointer());
        client.writeBytes(keyStream);

        storage.endRequest(client);

        byte[] stream;
        try {
          storage.beginResponse(client);
          stream = client.readBytes();
        } finally {
          storage.endResponse(client);
        }

        final byte serializerId = OByteSerializer.INSTANCE.deserializeLiteral(stream, 0);
        final OBinarySerializer<V> serializer = (OBinarySerializer<V>) OBinarySerializerFactory.getInstance().getObjectSerializer(
          serializerId);
        return serializer.deserialize(stream, OByteSerializer.BYTE_SIZE);
      }
    },"Cannot get by key from sb-tree bonsai");
  }

  @Override
  public boolean put(K key, V value) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public V remove(K key) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void delete() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public long size() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Collection<V> getValuesMinor(K key, boolean inclusive, int maxValuesToFetch) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void loadEntriesMinor(K key, boolean inclusive, RangeResultListener<K, V> listener) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Collection<V> getValuesMajor(K key, boolean inclusive, int maxValuesToFetch) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void loadEntriesMajor(K key, boolean inclusive, boolean ascSortOrder, RangeResultListener<K, V> listener) {
    if (!ascSortOrder)
      throw new IllegalStateException("Descending sort order is not supported");

    List<Map.Entry<K, V>> entries = fetchEntriesMajor(key, inclusive);

    while (pushEntriesToListener(listener, entries)) {
      final K nextKey = entries.get(entries.size() - 1).getKey();
      entries = fetchEntriesMajor(nextKey, false);
    }
  }

  private boolean pushEntriesToListener(RangeResultListener<K, V> listener, List<Map.Entry<K, V>> entries) {
    boolean more = false;
    for (Map.Entry<K, V> entry : entries) {
      more = listener.addResult(entry);

      if (!more)
        return false;
    }
    return more;
  }

  private List<Map.Entry<K, V>> fetchEntriesMajor(final K key,final boolean inclusive) {
    final byte[] keyStream = new byte[keySerializer.getObjectSize(key)];
    keySerializer.serialize(key, keyStream, 0);
    final OStorageRemote storage = (OStorageRemote) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying();
    return storage.networkOperation(new OStorageRemoteOperation<List<Map.Entry<K, V>>>() {
      @Override
      public List<Map.Entry<K, V>> execute(final OChannelBinaryAsynchClient client) throws IOException {
        try {
          storage.beginRequest(client, OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET_ENTRIES_MAJOR);
          OCollectionNetworkSerializer.INSTANCE.writeCollectionPointer(client, getCollectionPointer());
          client.writeBytes(keyStream);
          client.writeBoolean(inclusive);

          if (client.getSrvProtocolVersion() >= 21)
            client.writeInt(128);
        }finally {
          storage.endRequest(client);
        }
        List<Map.Entry<K, V>> list = null;
        try {
          storage.beginResponse(client);
          byte[] stream = client.readBytes();
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
        } finally {
          storage.endResponse(client);
        }

        return list;
      }
    },"Cannot get first key from sb-tree bonsai");
  }

  @Override
  public Collection<V> getValuesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, int maxValuesToFetch) {

    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public K firstKey() {
    final OStorageRemote storage = (OStorageRemote) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying();
    return storage.networkOperation(new OStorageRemoteOperation<K>() {
      @Override
      public K execute(final OChannelBinaryAsynchClient client) throws IOException {
        storage.beginRequest(client,OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_FIRST_KEY);
        OCollectionNetworkSerializer.INSTANCE.writeCollectionPointer(client, getCollectionPointer());
        storage.endRequest(client);
        byte[] stream;
        try {
          storage.beginResponse(client);
          stream = client.readBytes();
        } finally {
          storage.endResponse(client);
        }

        final byte serializerId = OByteSerializer.INSTANCE.deserializeLiteral(stream, 0);
        final OBinarySerializer<K> serializer = (OBinarySerializer<K>) OBinarySerializerFactory.getInstance().getObjectSerializer(
          serializerId);
        return serializer.deserialize(stream, OByteSerializer.BYTE_SIZE);
      }
    },"Cannot get first key from sb-tree bonsai");
  }

  @Override
  public K lastKey() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void loadEntriesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, RangeResultListener<K, V> listener) {

    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public int getRealBagSize(final Map<K, OSBTreeRidBag.Change> changes) {
    final OStorageRemote storage = (OStorageRemote) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying();
    return storage.networkOperation(new OStorageRemoteOperation<Integer>() {
      @Override
      public Integer execute(OChannelBinaryAsynchClient client) throws IOException {
        try {
          storage.beginRequest(client, OChannelBinaryProtocol.REQUEST_RIDBAG_GET_SIZE);
          OCollectionNetworkSerializer.INSTANCE.writeCollectionPointer(client, getCollectionPointer());

          final OSBTreeRidBag.ChangeSerializationHelper changeSerializer = OSBTreeRidBag.ChangeSerializationHelper.INSTANCE;
          final byte[] stream = new byte[OIntegerSerializer.INT_SIZE + changeSerializer.getChangesSerializedSize(changes.size())];
          changeSerializer.serializeChanges(changes, keySerializer, stream, 0);

          client.writeBytes(stream);
        } finally {
          storage.endRequest(client);
        }
        int result;
        try {
          storage.beginResponse(client);
          result = client.readInt();
        } finally {
          storage.endResponse(client);
        }
        return result;
      }
    }, "Cannot get by real bag size sb-tree bonsai");
  }

  @Override
  public OBinarySerializer<K> getKeySerializer() {
    return keySerializer;
  }

  @Override
  public OBinarySerializer<V> getValueSerializer() {
    return valueSerializer;
  }

  class TreeEntry<EK, EV> implements Map.Entry<EK, EV> {
    private final EK key;
    private final EV value;

    TreeEntry(EK key, EV value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public EK getKey() {
      return key;
    }

    @Override
    public EV getValue() {
      return value;
    }

    @Override
    public EV setValue(EV value) {
      throw new UnsupportedOperationException();
    }
  }
}

package com.orientechnologies.orient.client.remote;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeRidBag;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryAsynchClient;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;

/**
 * Implementation of {@link OSBTreeBonsai} for remote storage.
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
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
    OStorageRemote storage = (OStorageRemote) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying();

    final byte[] keyStream = new byte[keySerializer.getObjectSize(key)];
    keySerializer.serialize(key, keyStream, 0);
    try {
      OChannelBinaryAsynchClient client = storage.beginRequest(OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET);
      OCollectionNetworkSerializer.INSTANCE.writeCollectionPointer(client, getCollectionPointer());
      client.writeBytes(keyStream);

      storage.endRequest(client);

      storage.beginResponse(client);
      byte[] stream = client.readBytes();
      storage.endResponse(client);

      final byte serializerId = OByteSerializer.INSTANCE.deserialize(stream, 0);
      final OBinarySerializer<V> serializer = (OBinarySerializer<V>) OBinarySerializerFactory.getInstance().getObjectSerializer(
          serializerId);
      return serializer.deserialize(stream, OByteSerializer.BYTE_SIZE);
    } catch (IOException e) {
      throw new ODatabaseException("Can't get first key from sb-tree bonsai.", e);
    }
  }

  @Override
  public boolean put(K key, V value) {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public V remove(K key) {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public void delete() {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public long size() {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public Collection<V> getValuesMinor(K key, boolean inclusive, int maxValuesToFetch) {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public void loadEntriesMinor(K key, boolean inclusive, RangeResultListener<K, V> listener) {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public Collection<V> getValuesMajor(K key, boolean inclusive, int maxValuesToFetch) {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public void loadEntriesMajor(K key, boolean inclusive, boolean ascSortOrder, RangeResultListener<K, V> listener) {
    if (!ascSortOrder)
      throw new IllegalStateException("Descending sort order is not supported.");

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

  private List<Map.Entry<K, V>> fetchEntriesMajor(K key, boolean inclusive) {
    byte[] keyStream = new byte[keySerializer.getObjectSize(key)];
    keySerializer.serialize(key, keyStream, 0);

    OStorageRemote storage = (OStorageRemote) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying();
    try {
      OChannelBinaryAsynchClient client = storage.beginRequest(OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET_ENTRIES_MAJOR);
      OCollectionNetworkSerializer.INSTANCE.writeCollectionPointer(client, getCollectionPointer());
      client.writeBytes(keyStream);
      client.writeBoolean(inclusive);

      storage.endRequest(client);

      storage.beginResponse(client);
      byte[] stream = client.readBytes();
      int offset = 0;
      final int count = OIntegerSerializer.INSTANCE.deserialize(stream, 0);
      offset += OIntegerSerializer.INT_SIZE;

      List<Map.Entry<K, V>> list = new ArrayList<Map.Entry<K, V>>(count);
      for (int i = 0; i < count; i++) {
        final K resultKey = keySerializer.deserialize(stream, offset);
        offset += keySerializer.getObjectSize(stream, offset);
        final V resultValue = valueSerializer.deserialize(stream, offset);
        offset += valueSerializer.getObjectSize(stream, offset);

        list.add(new TreeEntry<K, V>(resultKey, resultValue));
      }

      storage.endResponse(client);

      return list;
    } catch (IOException e) {
      throw new ODatabaseException("Can't get first key from sb-tree bonsai.", e);
    }
  }

  @Override
  public Collection<V> getValuesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, int maxValuesToFetch) {

    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public K firstKey() {
    OStorageRemote storage = (OStorageRemote) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying();

    try {
      OChannelBinaryAsynchClient client = storage.beginRequest(OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_FIRST_KEY);
      OCollectionNetworkSerializer.INSTANCE.writeCollectionPointer(client, getCollectionPointer());
      storage.endRequest(client);

      storage.beginResponse(client);
      byte[] stream = client.readBytes();
      storage.endResponse(client);

      final byte serializerId = OByteSerializer.INSTANCE.deserialize(stream, 0);
      final OBinarySerializer<K> serializer = (OBinarySerializer<K>) OBinarySerializerFactory.getInstance().getObjectSerializer(
          serializerId);
      return serializer.deserialize(stream, OByteSerializer.BYTE_SIZE);
    } catch (IOException e) {
      throw new ODatabaseException("Can't get first key from sb-tree bonsai.", e);
    }
  }

  @Override
  public K lastKey() {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public void loadEntriesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, RangeResultListener<K, V> listener) {

    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public int getRealBagSize(Map<K, OSBTreeRidBag.Change> changes) {
    OStorageRemote storage = (OStorageRemote) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying();

    try {
      OChannelBinaryAsynchClient client = storage.beginRequest(OChannelBinaryProtocol.REQUEST_RIDBAG_GET_SIZE);
      OCollectionNetworkSerializer.INSTANCE.writeCollectionPointer(client, getCollectionPointer());

      final OSBTreeRidBag.ChangeSerializationHelper changeSerializer = OSBTreeRidBag.ChangeSerializationHelper.INSTANCE;
      final byte[] stream = new byte[OIntegerSerializer.INT_SIZE + changeSerializer.getChangesSerializedSize(changes.size())];
      changeSerializer.serializeChanges(changes, keySerializer, stream, 0);

      client.writeBytes(stream);

      storage.endRequest(client);

      storage.beginResponse(client);
      int result = client.readInt();
      storage.endResponse(client);

      return result;
    } catch (IOException e) {
      throw new ODatabaseException("Can't get first key from sb-tree bonsai.", e);
    }
  }

  @Override
  public OBinarySerializer<K> getKeySerializer() {
    return keySerializer;
  }

  @Override
  public OBinarySerializer<V> getValueSerializer() {
    return valueSerializer;
  }

  class TreeEntry<K, V> implements Map.Entry<K, V> {
    private final K key;
    private final V value;

    TreeEntry(K key, V value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public V setValue(V value) {
      throw new UnsupportedOperationException();
    }
  }
}

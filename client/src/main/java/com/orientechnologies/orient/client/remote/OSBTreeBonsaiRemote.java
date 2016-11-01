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

package com.orientechnologies.orient.client.remote;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.orient.client.remote.message.OSBTFetchEntriesMajorRequest;
import com.orientechnologies.orient.client.remote.message.OSBTFetchEntriesMajorResponse;
import com.orientechnologies.orient.client.remote.message.OSBTFirstKeyRequest;
import com.orientechnologies.orient.client.remote.message.OSBTFirstKeyResponse;
import com.orientechnologies.orient.client.remote.message.OSBTGetRealBagSizeRequest;
import com.orientechnologies.orient.client.remote.message.OSBTGetRealBagSizeResponse;
import com.orientechnologies.orient.client.remote.message.OSBTGetRequest;
import com.orientechnologies.orient.client.remote.message.OSBTGetResponse;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeRidBag;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeRidBag.Change;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;

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
    final OBonsaiCollectionPointer collectionPointer = getCollectionPointer();
    OSBTGetRequest request = new OSBTGetRequest(collectionPointer, keyStream);

    OSBTGetResponse response = storage.networkOperation(request, "Cannot get by key from sb-tree bonsai");

    byte[] stream = response.getStream();
    final byte serializerId = OByteSerializer.INSTANCE.deserializeLiteral(stream, 0);
    final OBinarySerializer<V> serializer = (OBinarySerializer<V>) OBinarySerializerFactory.getInstance()
        .getObjectSerializer(serializerId);
    return serializer.deserialize(stream, OByteSerializer.BYTE_SIZE);
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

  private List<Map.Entry<K, V>> fetchEntriesMajor(final K key, final boolean inclusive) {
    final byte[] keyStream = new byte[keySerializer.getObjectSize(key)];
    keySerializer.serialize(key, keyStream, 0);
    final OStorageRemote storage = (OStorageRemote) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying();
    OSBTFetchEntriesMajorRequest<K, V> request = new OSBTFetchEntriesMajorRequest<K, V>(inclusive, keyStream,
        getCollectionPointer(), keySerializer, valueSerializer);
    OSBTFetchEntriesMajorResponse<K, V> response = storage.networkOperation(request, "Cannot get first key from sb-tree bonsai");

    return response.getList();
  }

  @Override
  public Collection<V> getValuesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, int maxValuesToFetch) {

    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public K firstKey() {
    final OStorageRemote storage = (OStorageRemote) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying();
    final OBonsaiCollectionPointer collectionPointer = getCollectionPointer();
    OSBTFirstKeyRequest request = new OSBTFirstKeyRequest(collectionPointer);
    OSBTFirstKeyResponse response = storage.networkOperation(request, "Cannot get first key from sb-tree bonsai");

    byte[] stream = response.getStream();
    final byte serializerId = OByteSerializer.INSTANCE.deserializeLiteral(stream, 0);
    final OBinarySerializer<K> serializer = (OBinarySerializer<K>) OBinarySerializerFactory.getInstance()
        .getObjectSerializer(serializerId);
    return serializer.deserialize(stream, OByteSerializer.BYTE_SIZE);

  }

  @Override
  public K lastKey() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void loadEntriesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive,
      RangeResultListener<K, V> listener) {

    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public int getRealBagSize(final Map<K, OSBTreeRidBag.Change> changes) {
    final OStorageRemote storage = (OStorageRemote) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying();
    final OBonsaiCollectionPointer collectionPointer = getCollectionPointer();
    OSBTGetRealBagSizeRequest request = new OSBTGetRealBagSizeRequest((OBinarySerializer<OIdentifiable>) keySerializer,
        collectionPointer, (Map<OIdentifiable, Change>) changes);
    OSBTGetRealBagSizeResponse response = storage.networkOperation(request, "Cannot get by real bag size sb-tree bonsai");
    return response.getRealSize();
  }

  @Override
  public OBinarySerializer<K> getKeySerializer() {
    return keySerializer;
  }

  @Override
  public OBinarySerializer<V> getValueSerializer() {
    return valueSerializer;
  }

  public static class TreeEntry<EK, EV> implements Map.Entry<EK, EV> {
    private final EK key;
    private final EV value;

    public TreeEntry(EK key, EV value) {
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

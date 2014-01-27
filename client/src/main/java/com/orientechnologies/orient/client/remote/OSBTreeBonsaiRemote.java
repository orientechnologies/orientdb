package com.orientechnologies.orient.client.remote;

import java.util.Collection;
import java.util.Map;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeRidBag;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;

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
    throw new UnsupportedOperationException("Not implemented yet.");
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
  public void loadEntriesMajor(K key, boolean inclusive, RangeResultListener<K, V> listener) {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public Collection<V> getValuesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, int maxValuesToFetch) {

    throw new UnsupportedOperationException("Not implemented yet.");
  }

  @Override
  public K firstKey() {
    throw new UnsupportedOperationException("Not implemented yet.");
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
    throw new UnsupportedOperationException("Not implemented yet.");
  }
}

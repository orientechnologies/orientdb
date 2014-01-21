package com.orientechnologies.orient.core.index.sbtreebonsai.local;

import java.util.Collection;
import java.util.Map;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeRidBag;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeBonsaiRemote<K, V> implements OSBTreeBonsai<K, V> {

  @Override
  public void create(String name, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer) {

  }

  @Override
  public void create(long fileId, OBinarySerializer<K> keySerializer, OBinarySerializer<V> valueSerializer) {

  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public long getFileId() {
    return 0;
  }

  @Override
  public OBonsaiBucketPointer getRootBucketPointer() {
    return null;
  }

  @Override
  public V get(K key) {
    return null;
  }

  @Override
  public boolean put(K key, V value) {
    return false;
  }

  @Override
  public void close(boolean flush) {

  }

  @Override
  public void close() {

  }

  @Override
  public void clear() {

  }

  @Override
  public void delete() {

  }

  @Override
  public void load(long fileId, OBonsaiBucketPointer rootBucketPointer, OStorageLocalAbstract storageLocal) {

  }

  @Override
  public long size() {
    return 0;
  }

  @Override
  public V remove(K key) {
    return null;
  }

  @Override
  public Collection<V> getValuesMinor(K key, boolean inclusive, int maxValuesToFetch) {
    return null;
  }

  @Override
  public void loadEntriesMinor(K key, boolean inclusive, RangeResultListener<K, V> listener) {

  }

  @Override
  public Collection<V> getValuesMajor(K key, boolean inclusive, int maxValuesToFetch) {
    return null;
  }

  @Override
  public void loadEntriesMajor(K key, boolean inclusive, RangeResultListener<K, V> listener) {

  }

  @Override
  public Collection<V> getValuesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, int maxValuesToFetch) {
    return null;
  }

  @Override
  public K firstKey() {
    return null;
  }

  @Override
  public K lastKey() {
    return null;
  }

  @Override
  public void loadEntriesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, RangeResultListener<K, V> listener) {

  }

  @Override
  public void flush() {

  }

  @Override
  public int getRealBagSize(Map<K, OSBTreeRidBag.Change> changes) {
    return 0;
  }
}

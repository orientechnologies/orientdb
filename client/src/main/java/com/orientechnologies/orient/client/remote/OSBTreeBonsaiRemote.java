package com.orientechnologies.orient.client.remote;

import java.util.Collection;
import java.util.Map;

import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeRidBag;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeBonsaiRemote<K, V> implements OSBTreeBonsai<K, V> {
  private final OBonsaiCollectionPointer treePointer;

  public OSBTreeBonsaiRemote(OBonsaiCollectionPointer treePointer) {
    this.treePointer = treePointer;
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
  public V get(K key) {
    return null;
  }

  @Override
  public boolean put(K key, V value) {
    return false;
  }

  @Override
  public V remove(K key) {
    return null;
  }

  @Override
  public void clear() {

  }

  @Override
  public void delete() {

  }

  @Override
  public long size() {
    return 0;
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
  public int getRealBagSize(Map<K, OSBTreeRidBag.Change> changes) {
    return 0;
  }
}

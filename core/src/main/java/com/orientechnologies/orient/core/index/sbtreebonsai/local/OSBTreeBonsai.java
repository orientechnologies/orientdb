package com.orientechnologies.orient.core.index.sbtreebonsai.local;

import java.util.Collection;
import java.util.Map;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeRidBag;
import com.orientechnologies.orient.core.index.sbtree.OTreeInternal;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public interface OSBTreeBonsai<K, V> extends OTreeInternal<K, V> {
  long getFileId();

  OBonsaiBucketPointer getRootBucketPointer();

  OBonsaiCollectionPointer getCollectionPointer();

  V get(K key);

  boolean put(K key, V value);

  void clear();

  void delete();

  long size();

  V remove(K key);

  Collection<V> getValuesMinor(K key, boolean inclusive, int maxValuesToFetch);

  void loadEntriesMinor(K key, boolean inclusive, RangeResultListener<K, V> listener);

  Collection<V> getValuesMajor(K key, boolean inclusive, int maxValuesToFetch);

  void loadEntriesMajor(K key, boolean inclusive, RangeResultListener<K, V> listener);

  Collection<V> getValuesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, int maxValuesToFetch);

  K firstKey();

  K lastKey();

  void loadEntriesBetween(K keyFrom, boolean fromInclusive, K keyTo, boolean toInclusive, RangeResultListener<K, V> listener);

  /**
   * Hardcoded method for Bag to avoid creation of extra layer.
   * <p/>
   * Don't make any changes to tree.
   * 
   * @param changes
   *          Bag changes
   * @return real bag size
   */
  int getRealBagSize(Map<K, OSBTreeRidBag.Change> changes);

  OBinarySerializer<K> getKeySerializer();

  OBinarySerializer<V> getValueSerializer();
}

package com.orientechnologies.orient.core.index.sbtree;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public interface OTree<K, V> {
  long size();

  void loadEntriesMajor(K key, boolean inclusive, RangeResultListener<K, V> listener);

  K firstKey();

  V remove(K key);

  /**
   * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
   */
  interface RangeResultListener<K, V> {
    boolean addResult(BucketEntry<K, V> entry);
  }

  interface BucketEntry<K, V> {
    V getValue();

    K getKey();
  }
}

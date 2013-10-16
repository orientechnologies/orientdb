package com.orientechnologies.orient.core.index.sbtree;

import java.util.Map;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public interface OTreeInternal<K, V> {
  long size();

  void loadEntriesMajor(K key, boolean inclusive, RangeResultListener<K, V> listener);

  K firstKey();

  V remove(K key);

  /**
   * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
   */
  interface RangeResultListener<K, V> {
    boolean addResult(Map.Entry<K, V> entry);
  }
}

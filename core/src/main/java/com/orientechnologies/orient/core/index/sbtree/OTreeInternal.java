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
    /**
     * Callback method for result entries.
     * 
     * @param entry
     *          result entry
     * @return true if continue to iterate through entries, false if no more result needed.
     */
    boolean addResult(Map.Entry<K, V> entry);
  }
}

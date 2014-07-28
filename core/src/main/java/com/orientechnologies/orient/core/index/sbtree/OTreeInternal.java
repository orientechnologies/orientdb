package com.orientechnologies.orient.core.index.sbtree;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public interface OTreeInternal<K, V> {
  long size();

  void loadEntriesMajor(K key, boolean inclusive, boolean ascSortOrder, RangeResultListener<K, V> listener);

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

  public class AccumulativeListener<K, V> implements RangeResultListener<K, V> {
    private final int             limit;
    private List<Map.Entry<K, V>> entries;

    public AccumulativeListener(int limit) {
      entries = new ArrayList<Map.Entry<K, V>>(limit);
      this.limit = limit;
    }

    @Override
    public boolean addResult(Map.Entry<K, V> entry) {
      entries.add(entry);

      return limit > entries.size();
    }

    public List<Map.Entry<K, V>> getResult() {
      return entries;
    }
  }
}

/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
  *  * For more information: http://www.orientechnologies.com
  *
  */

package com.orientechnologies.orient.core.index.sbtree;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public interface OTreeInternal<K, V> {
  long size();

  void loadEntriesMajor(K key, boolean inclusive, boolean ascSortOrder, RangeResultListener<K, V> listener);

  K firstKey();

  V remove(K key);

  /**
   * @author Artem Orobets (enisher-at-gmail.com)
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

/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.index.sbtree;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeMapEntryIterator<K, V> implements Iterator<Map.Entry<K, V>> {
  private LinkedList<Map.Entry<K, V>> preFetchedValues;
  private final OTree<K, V>           sbTree;
  private K                           firstKey;
  private Map.Entry<K, V>             currentEntry;

  public OSBTreeMapEntryIterator(OTree<K, V> sbTree) {
    this.sbTree = sbTree;
    if (sbTree.size() == 0) {
      this.preFetchedValues = null;
      return;
    }

    this.preFetchedValues = new LinkedList<Map.Entry<K, V>>();
    firstKey = sbTree.firstKey();

    prefetchData(true);
  }

  private void prefetchData(boolean firstTime) {
    sbTree.loadEntriesMajor(firstKey, firstTime, new OTree.RangeResultListener<K, V>() {
      @Override
      public boolean addResult(final OTree.BucketEntry<K, V> entry) {
        preFetchedValues.add(new Map.Entry<K, V>() {
          @Override
          public K getKey() {
            return entry.getKey();
          }

          @Override
          public V getValue() {
            return entry.getValue();
          }

          @Override
          public V setValue(V v) {
            throw new UnsupportedOperationException("setValue");
          }
        });

        return preFetchedValues.size() <= 8000;
      }
    });

    if (preFetchedValues.isEmpty())
      preFetchedValues = null;
    else
      firstKey = preFetchedValues.getLast().getKey();
  }

  @Override
  public boolean hasNext() {
    return preFetchedValues != null;
  }

  @Override
  public Map.Entry<K, V> next() {
    final Map.Entry<K, V> entry = preFetchedValues.removeFirst();
    if (preFetchedValues.isEmpty())
      prefetchData(false);

    currentEntry = entry;

    return entry;
  }

  @Override
  public void remove() {
    sbTree.remove(currentEntry.getKey());
    currentEntry = null;
  }
}

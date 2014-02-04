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

import com.orientechnologies.orient.core.index.sbtree.local.OSBTree;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeInverseMapEntryIterator<K, V> implements Iterator<Map.Entry<K, V>> {
  private final OSBTree<K, V>         sbTree;
  private LinkedList<Map.Entry<K, V>> preFetchedValues;
  private K                           lastKey;

  public OSBTreeInverseMapEntryIterator(OSBTree<K, V> sbTree) {
    this.sbTree = sbTree;

    if (sbTree.size() == 0) {
      this.preFetchedValues = null;
      return;
    }

    this.preFetchedValues = new LinkedList<Map.Entry<K, V>>();
    lastKey = sbTree.lastKey();

    prefetchData(true);
  }

  private void prefetchData(boolean firstTime) {
    sbTree.loadEntriesMinor(lastKey, firstTime, new OSBTree.RangeResultListener<K, V>() {
      @Override
      public boolean addResult(final Map.Entry<K, V> entry) {
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
      lastKey = preFetchedValues.getLast().getKey();
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

    return entry;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove");
  }
}

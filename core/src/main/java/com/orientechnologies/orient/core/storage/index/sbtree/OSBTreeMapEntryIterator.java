/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.index.sbtree;

import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OMixedIndexRIDContainer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;

/** @author Artem Orobets (enisher-at-gmail.com) */
public class OSBTreeMapEntryIterator<K, V> implements Iterator<Map.Entry<K, V>> {
  private LinkedList<Map.Entry<K, V>> preFetchedValues;
  private final OTreeInternal<K, V> sbTree;
  private K firstKey;

  private final int prefetchSize;

  public OSBTreeMapEntryIterator(OTreeInternal<K, V> sbTree) {
    this(sbTree, 8000);
  }

  private OSBTreeMapEntryIterator(OTreeInternal<K, V> sbTree, int prefetchSize) {
    this.sbTree = sbTree;
    this.prefetchSize = prefetchSize;

    if (sbTree.isEmpty()) {
      this.preFetchedValues = null;
      return;
    }

    this.preFetchedValues = new LinkedList<>();
    firstKey = sbTree.firstKey();

    prefetchData(true);
  }

  private void prefetchData(boolean firstTime) {
    ODatabaseInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    long begin = System.currentTimeMillis();
    try {
      sbTree.loadEntriesMajor(
          firstKey,
          firstTime,
          true,
          entry -> {
            final V value = entry.getValue();
            final V resultValue;
            if (value instanceof OIndexRIDContainer || value instanceof OMixedIndexRIDContainer) {
              //noinspection unchecked
              resultValue =
                  (V) new HashSet<OIdentifiable>((Collection<? extends OIdentifiable>) value);
            } else {
              resultValue = value;
            }

            preFetchedValues.add(
                new Map.Entry<K, V>() {
                  @Override
                  public K getKey() {
                    return entry.getKey();
                  }

                  @Override
                  public V getValue() {
                    return resultValue;
                  }

                  @Override
                  public V setValue(V v) {
                    throw new UnsupportedOperationException("setValue");
                  }
                });

            return preFetchedValues.size() <= prefetchSize;
          });

      if (preFetchedValues.isEmpty()) {
        preFetchedValues = null;
      } else {
        firstKey = preFetchedValues.getLast().getKey();
      }
    } finally {
      if (db != null) {
        db.addRidbagPrefetchStats(System.currentTimeMillis() - begin);
      }
    }
  }

  @Override
  public boolean hasNext() {
    return preFetchedValues != null;
  }

  @Override
  public Map.Entry<K, V> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    final Map.Entry<K, V> entry = preFetchedValues.removeFirst();
    if (preFetchedValues.isEmpty()) {
      prefetchData(false);
    }

    return entry;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}

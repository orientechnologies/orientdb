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
package com.orientechnologies.orient.core.tx;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Collects the changes to an index for a certain key
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OTransactionIndexChanges {

  public enum OPERATION {
    PUT,
    REMOVE,
    CLEAR
  }

  public NavigableMap<Object, OTransactionIndexChangesPerKey> changesPerKey =
      new TreeMap<Object, OTransactionIndexChangesPerKey>(ODefaultComparator.INSTANCE);

  public OTransactionIndexChangesPerKey nullKeyChanges = new OTransactionIndexChangesPerKey(null);

  public boolean cleared = false;

  private OIndexInternal resolvedIndex = null;

  public OTransactionIndexChangesPerKey getChangesPerKey(final Object key) {
    if (key == null) return nullKeyChanges;

    return changesPerKey.computeIfAbsent(key, OTransactionIndexChangesPerKey::new);
  }

  public void setCleared() {
    changesPerKey.clear();
    nullKeyChanges.clear();

    cleared = true;
  }

  public Object getFirstKey() {
    return changesPerKey.firstKey();
  }

  public Object getLastKey() {
    return changesPerKey.lastKey();
  }

  public Object getLowerKey(Object key) {
    return changesPerKey.lowerKey(key);
  }

  public Object getHigherKey(Object key) {
    return changesPerKey.higherKey(key);
  }

  public Object getCeilingKey(Object key) {
    return changesPerKey.ceilingKey(key);
  }

  public Object[] firstAndLastKeys(
      Object from, boolean fromInclusive, Object to, boolean toInclusive) {
    final NavigableMap<Object, OTransactionIndexChangesPerKey> interval;
    if (from != null && to != null) {
      interval = changesPerKey.subMap(from, fromInclusive, to, toInclusive);
    } else if (from != null) {
      interval = changesPerKey.headMap(from, fromInclusive);
    } else if (to != null) {
      interval = changesPerKey.tailMap(to, toInclusive);
    } else {
      interval = changesPerKey;
    }

    if (interval.isEmpty()) {
      return new Object[0];
    } else {
      return new Object[] {interval.firstKey(), interval.lastKey()};
    }
  }

  public Object getFloorKey(Object key) {
    return changesPerKey.floorKey(key);
  }

  public OIndexInternal resolveAssociatedIndex(
      String indexName, OIndexManagerAbstract indexManager, ODatabaseDocumentInternal db) {
    if (resolvedIndex == null) {
      final OIndex index = indexManager.getIndex(db, indexName);
      if (index != null) resolvedIndex = index.getInternal();
    }

    return resolvedIndex;
  }

  public OIndexInternal getAssociatedIndex() {
    return resolvedIndex;
  }
}

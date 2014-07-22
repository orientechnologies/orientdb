/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.engine.local.OEngineLocal;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerRID;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;

/**
 * Abstract Index implementation that allows only one value for a key.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OIndexOneValue extends OIndexAbstract<OIdentifiable> {
  public OIndexOneValue(final String type, String algorithm, OIndexEngine<OIdentifiable> engine, String valueContainerAlgorithm) {
    super(type, algorithm, engine, valueContainerAlgorithm);
  }

  public OIdentifiable get(Object iKey) {
    checkForRebuild();

    iKey = getCollatingValue(iKey);

    acquireSharedLock();
    try {
      return indexEngine.get(iKey);
    } finally {
      releaseSharedLock();
    }
  }

  public long count(Object iKey) {
    checkForRebuild();

    iKey = getCollatingValue(iKey);

    acquireSharedLock();
    try {
      return indexEngine.contains(iKey) ? 1 : 0;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void checkEntry(final OIdentifiable record, Object key) {
    checkForRebuild();

    key = getCollatingValue(key);

    // CHECK IF ALREADY EXIST
    final OIdentifiable indexedRID = get(key);
    if (indexedRID != null && !indexedRID.getIdentity().equals(record.getIdentity())) {
      // CHECK IF IN THE SAME TX THE ENTRY WAS DELETED
      String storageType = getDatabase().getStorage().getType();
      if (storageType.equals(OEngineMemory.NAME) || storageType.equals(OEngineLocal.NAME)) {
        final OTransactionIndexChanges indexChanges = ODatabaseRecordThreadLocal.INSTANCE.get().getTransaction()
            .getIndexChanges(getName());
        if (indexChanges != null) {
          final OTransactionIndexChangesPerKey keyChanges = indexChanges.getChangesPerKey(key);
          if (keyChanges != null) {
            for (OTransactionIndexChangesPerKey.OTransactionIndexEntry entry : keyChanges.entries) {
              if (entry.operation == OTransactionIndexChanges.OPERATION.REMOVE)
                // WAS DELETED, OK!
                return;
            }
          }
        }
      }

      throw new OIndexException("Cannot index record : " + record + " found duplicated key '" + key + "' in index " + getName()
          + " previously assigned to the record " + indexedRID);
    }
  }

  public OIndexOneValue create(final String name, final OIndexDefinition indexDefinition, final String clusterIndexName,
      final Set<String> clustersToIndex, boolean rebuild, final OProgressListener progressListener) {
    return (OIndexOneValue) super.create(name, indexDefinition, clusterIndexName, clustersToIndex, rebuild, progressListener,
        determineValueSerializer());
  }

  @Override
  public OIndexCursor iterateEntries(Collection<?> keys, boolean ascSortOrder) {
    checkForRebuild();

    final List<Object> sortedKeys = new ArrayList<Object>(keys);
    final Comparator<Object> comparator;

    if (ascSortOrder)
      comparator = ODefaultComparator.INSTANCE;
    else
      comparator = Collections.reverseOrder(ODefaultComparator.INSTANCE);

    Collections.sort(sortedKeys, comparator);

    return new OIndexAbstractCursor() {
      private Iterator<?> keysIterator = sortedKeys.iterator();

      @Override
      public Map.Entry<Object, OIdentifiable> nextEntry() {
        OIdentifiable result = null;
        Object key = null;
        while (keysIterator.hasNext() && result == null) {
          key = keysIterator.next();
          key = getCollatingValue(key);

          acquireSharedLock();
          try {
            result = indexEngine.get(key);
          } finally {
            releaseSharedLock();
          }
        }

        if (result == null)
          return null;

        final Object resultKey = key;
        final OIdentifiable resultValue = result;

        return new Map.Entry<Object, OIdentifiable>() {
          @Override
          public Object getKey() {
            return resultKey;
          }

          @Override
          public OIdentifiable getValue() {
            return resultValue;
          }

          @Override
          public OIdentifiable setValue(OIdentifiable value) {
            throw new UnsupportedOperationException("setValue");
          }
        };
      }
    };
  }

  @Override
  public OIndexCursor iterateEntriesBetween(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive,
      boolean ascOrder) {
    checkForRebuild();

    fromKey = getCollatingValue(fromKey);
    toKey = getCollatingValue(toKey);

    acquireSharedLock();
    try {
      return indexEngine.iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder, null);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    checkForRebuild();

    fromKey = getCollatingValue(fromKey);
    acquireSharedLock();
    try {
      return indexEngine.iterateEntriesMajor(fromKey, fromInclusive, ascOrder, null);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    checkForRebuild();

    toKey = getCollatingValue(toKey);
    acquireSharedLock();
    try {
      return indexEngine.iterateEntriesMinor(toKey, toInclusive, ascOrder, null);
    } finally {
      releaseSharedLock();
    }
  }

  public long getSize() {
    checkForRebuild();

    acquireExclusiveLock();
    try {
      return indexEngine.size(null);
    } finally {
      releaseExclusiveLock();
    }
  }

  public long getKeySize() {
    checkForRebuild();

    acquireExclusiveLock();
    try {
      return indexEngine.size(null);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public OIndexCursor cursor() {
    checkForRebuild();

    acquireSharedLock();
    try {
      return indexEngine.cursor(null);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor descCursor() {
    checkForRebuild();

    acquireSharedLock();
    try {
      return indexEngine.descCursor(null);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  protected OStreamSerializer determineValueSerializer() {
    return OStreamSerializerRID.INSTANCE;
  }
}

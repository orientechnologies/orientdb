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

import java.util.*;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.concur.resource.OSharedResourceIterator;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.engine.local.OEngineLocal;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.record.impl.ODocument;
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

  public OIdentifiable get(final Object iKey) {
    checkForRebuild();

    acquireSharedLock();
    try {
      return indexEngine.get(iKey);
    } finally {
      releaseSharedLock();
    }
  }

  public long count(final Object key) {
    checkForRebuild();

    acquireSharedLock();
    try {
      return indexEngine.contains(key) ? 1 : 0;
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public void checkEntry(final OIdentifiable iRecord, final Object key) {
    checkForRebuild();

    // CHECK IF ALREADY EXIST
    final OIdentifiable indexedRID = get(key);
    if (indexedRID != null && !indexedRID.getIdentity().equals(iRecord.getIdentity())) {
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

      OLogManager.instance().exception(
          "Cannot index record %s: found duplicated key '%s' in index '%s' previously assigned to the record %s", null,
          OIndexException.class, key, iRecord, indexedRID);
    }
  }

  public OIndexOneValue create(final String name, final OIndexDefinition indexDefinition, final String clusterIndexName,
      final Set<String> clustersToIndex, boolean rebuild, final OProgressListener progressListener) {
    return (OIndexOneValue) super.create(name, indexDefinition, clusterIndexName, clustersToIndex, rebuild, progressListener,
        OStreamSerializerRID.INSTANCE);
  }

  public void getValuesBetween(final Object iRangeFrom, final boolean iFromInclusive, final Object iRangeTo,
      final boolean iToInclusive, final IndexValuesResultListener resultListener) {
    checkForRebuild();

    if (iRangeFrom.getClass() != iRangeTo.getClass())
      throw new IllegalArgumentException("Range from-to parameters are of different types");

    acquireSharedLock();
    try {
      indexEngine.getValuesBetween(iRangeFrom, iFromInclusive, iRangeTo, iToInclusive, null,
          new OIndexEngine.ValuesResultListener() {
            @Override
            public boolean addResult(OIdentifiable identifiable) {
              return resultListener.addResult(identifiable);
            }
          });
    } finally {
      releaseSharedLock();
    }
  }

  public void getValuesMajor(final Object fromKey, final boolean isInclusive, final IndexValuesResultListener resultListener) {
    checkForRebuild();

    acquireSharedLock();
    try {
      indexEngine.getValuesMajor(fromKey, isInclusive, null, new OIndexEngine.ValuesResultListener() {
        @Override
        public boolean addResult(OIdentifiable identifiable) {
          return resultListener.addResult(identifiable);
        }
      });

    } finally {
      releaseSharedLock();
    }
  }

  public void getValuesMinor(final Object toKey, final boolean isInclusive, final IndexValuesResultListener resultListener) {
    checkForRebuild();

    acquireSharedLock();
    try {
      indexEngine.getValuesMinor(toKey, isInclusive, null, new OIndexEngine.ValuesResultListener() {
        @Override
        public boolean addResult(OIdentifiable identifiable) {
          return resultListener.addResult(identifiable);
        }
      });
    } finally {
      releaseSharedLock();
    }
  }

  public void getValues(final Collection<?> keys, final IndexValuesResultListener resultListener) {
    checkForRebuild();

    final List<Object> sortedKeys = new ArrayList<Object>(keys);
    Collections.sort(sortedKeys, ODefaultComparator.INSTANCE);

    acquireSharedLock();
    try {
      for (final Object key : sortedKeys) {
        final OIdentifiable val = indexEngine.get(key);
        if (val != null) {
          if (!resultListener.addResult(val))
            return;
        }
      }
    } finally {
      releaseSharedLock();
    }
  }

  public void getEntriesMajor(final Object fromKey, final boolean isInclusive,
      final IndexEntriesResultListener entriesResultListener) {
    checkForRebuild();

    acquireSharedLock();
    try {
      indexEngine.getEntriesMajor(fromKey, isInclusive, null, new OIndexEngine.EntriesResultListener() {
        @Override
        public boolean addResult(ODocument entry) {
          return entriesResultListener.addResult(entry);
        }
      });

    } finally {
      releaseSharedLock();
    }
  }

  public void getEntriesMinor(final Object toKey, final boolean isInclusive, final IndexEntriesResultListener entriesResultListener) {
    checkForRebuild();

    acquireSharedLock();
    try {
      indexEngine.getEntriesMinor(toKey, isInclusive, null, new OIndexEngine.EntriesResultListener() {
        @Override
        public boolean addResult(ODocument entry) {
          return entriesResultListener.addResult(entry);
        }
      });
    } finally {
      releaseSharedLock();
    }
  }

  public void getEntriesBetween(final Object rangeFrom, final Object rangeTo, final boolean inclusive,
      final IndexEntriesResultListener entriesResultListener) {
    checkForRebuild();

    if (rangeFrom.getClass() != rangeTo.getClass())
      throw new IllegalArgumentException("Range from-to parameters are of different types");

    acquireSharedLock();
    try {
      indexEngine.getEntriesBetween(rangeFrom, rangeTo, inclusive, null, new OIndexEngine.EntriesResultListener() {
        @Override
        public boolean addResult(ODocument entry) {
          return entriesResultListener.addResult(entry);
        }
      });
    } finally {
      releaseSharedLock();
    }
  }

  public void getEntries(final Collection<?> keys, IndexEntriesResultListener resultListener) {
    checkForRebuild();

    final List<Object> sortedKeys = new ArrayList<Object>(keys);
    Collections.sort(sortedKeys, ODefaultComparator.INSTANCE);

    acquireSharedLock();
    try {
      for (final Object key : sortedKeys) {
        final OIdentifiable val = indexEngine.get(key);
        if (val != null) {
          final ODocument document = new ODocument();
          document.field("key", key);
          document.field("rid", val.getIdentity());
          document.unsetDirty();

          if (!resultListener.addResult(document))
            return;
        }
      }
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

  public long count(final Object iRangeFrom, final boolean iFromInclusive, final Object iRangeTo, final boolean iToInclusive,
      final int maxValuesToFetch) {
    checkForRebuild();

    if (iRangeFrom != null && iRangeTo != null && iRangeFrom.getClass() != iRangeTo.getClass())
      throw new IllegalArgumentException("Range from-to parameters are of different types");

    acquireSharedLock();
    try {
      return indexEngine.count(iRangeFrom, iFromInclusive, iRangeTo, iToInclusive, maxValuesToFetch, null);
    } finally {
      releaseSharedLock();
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

  public Iterator<OIdentifiable> valuesIterator() {
    checkForRebuild();

    acquireSharedLock();
    try {
      return new OSharedResourceIterator<OIdentifiable>(this, indexEngine.valuesIterator());
    } finally {
      releaseSharedLock();
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public Iterator<OIdentifiable> valuesInverseIterator() {
    checkForRebuild();

    acquireSharedLock();
    try {
      return new OSharedResourceIterator<OIdentifiable>(this, indexEngine.inverseValuesIterator());
    } finally {
      releaseSharedLock();
    }
  }

}

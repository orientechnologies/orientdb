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
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerRID;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;

/**
 * Abstract Index implementation that allows only one value for a key.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OIndexOneValue extends OIndexAbstract<OIdentifiable> {
  public OIndexOneValue(final String iType, OIndexEngine<OIdentifiable> engine) {
    super(iType, engine);
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
      final OTransactionIndexChanges indexChanges = ODatabaseRecordThreadLocal.INSTANCE.get().getTransaction()
          .getIndexChanges(getName());
      if (indexChanges != null) {
        final OTransactionIndexChangesPerKey keyChanges = indexChanges.getChangesPerKey(key);
        if (keyChanges != null) {
          for (OTransactionIndexEntry entry : keyChanges.entries) {
            if (entry.operation == OPERATION.REMOVE)
              // WAS DELETED, OK!
              return;
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

  public Collection<OIdentifiable> getValuesBetween(final Object iRangeFrom, final boolean iFromInclusive, final Object iRangeTo,
      final boolean iToInclusive, final int maxValuesToFetch) {
    checkForRebuild();

    if (iRangeFrom.getClass() != iRangeTo.getClass())
      throw new IllegalArgumentException("Range from-to parameters are of different types");

    acquireSharedLock();
    try {
      return indexEngine.getValuesBetween(iRangeFrom, iFromInclusive, iRangeTo, iToInclusive, maxValuesToFetch, null);
    } finally {
      releaseSharedLock();
    }
  }

  public Collection<OIdentifiable> getValuesMajor(final Object fromKey, final boolean isInclusive, final int maxValuesToFetch) {
    checkForRebuild();

    acquireSharedLock();
    try {
      return indexEngine.getValuesMajor(fromKey, isInclusive, maxValuesToFetch, null);
    } finally {
      releaseSharedLock();
    }
  }

  public Collection<OIdentifiable> getValuesMinor(final Object toKey, final boolean isInclusive, final int maxValuesToFetch) {
    checkForRebuild();

    acquireSharedLock();
    try {
      return indexEngine.getValuesMinor(toKey, isInclusive, maxValuesToFetch, null);
    } finally {
      releaseSharedLock();
    }
  }

  public Collection<OIdentifiable> getValues(final Collection<?> keys, final int maxValuesToSearch) {
    checkForRebuild();

    final List<Object> sortedKeys = new ArrayList<Object>(keys);
    Collections.sort(sortedKeys, ODefaultComparator.INSTANCE);

    acquireSharedLock();
    final Set<OIdentifiable> result = new HashSet<OIdentifiable>();
    try {
      for (final Object key : sortedKeys) {
        if (maxValuesToSearch > -1 && result.size() == maxValuesToSearch)
          return result;

        final OIdentifiable val = indexEngine.get(key);
        if (val != null) {
          result.add(val);
        }
      }

      return result;
    } finally {
      releaseSharedLock();
    }
  }

  public Collection<ODocument> getEntriesMajor(final Object fromKey, final boolean isInclusive, final int maxEntriesToFetch) {
    checkForRebuild();

    acquireSharedLock();
    try {
      return indexEngine.getEntriesMajor(fromKey, isInclusive, maxEntriesToFetch, null);
    } finally {
      releaseSharedLock();
    }
  }

  public Collection<ODocument> getEntriesMinor(final Object toKey, final boolean isInclusive, final int maxEntriesToFetch) {
    checkForRebuild();

    acquireSharedLock();
    try {
      return indexEngine.getEntriesMinor(toKey, isInclusive, maxEntriesToFetch, null);
    } finally {
      releaseSharedLock();
    }
  }

  public Collection<ODocument> getEntriesBetween(final Object rangeFrom, final Object rangeTo, final boolean inclusive,
      final int maxEntriesToFetch) {
    checkForRebuild();

    if (rangeFrom.getClass() != rangeTo.getClass())
      throw new IllegalArgumentException("Range from-to parameters are of different types");

    acquireSharedLock();
    try {
      return indexEngine.getEntriesBetween(rangeFrom, rangeTo, inclusive, maxEntriesToFetch, null);
    } finally {
      releaseSharedLock();
    }
  }

  public Collection<ODocument> getEntries(final Collection<?> keys, final int maxEntriesToFetch) {
    checkForRebuild();

    final List<Object> sortedKeys = new ArrayList<Object>(keys);
    Collections.sort(sortedKeys, ODefaultComparator.INSTANCE);

    acquireSharedLock();
    final Set<ODocument> result = new ODocumentFieldsHashSet();
    try {
      for (final Object key : sortedKeys) {
        if (maxEntriesToFetch > -1 && result.size() == maxEntriesToFetch)
          return result;

        final OIdentifiable val = indexEngine.get(key);
        if (val != null) {
          final ODocument document = new ODocument();
          document.field("key", key);
          document.field("rid", val.getIdentity());
          document.unsetDirty();

          result.add(document);
        }
      }

      return result;
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

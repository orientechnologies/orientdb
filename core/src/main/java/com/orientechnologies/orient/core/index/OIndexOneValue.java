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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerRID;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Abstract Index implementation that allows only one value for a key.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OIndexOneValue extends OIndexAbstract<OIdentifiable> {
  public OIndexOneValue(String name, final String type, String algorithm, OIndexEngine<OIdentifiable> engine,
      String valueContainerAlgorithm, ODocument metadata, OStorage storage) {
    super(name, type, algorithm, engine, valueContainerAlgorithm, metadata, storage);
  }

  public OIdentifiable get(Object iKey) {
    iKey = getCollatingValue(iKey);

    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();
    if (!txIsActive)
      keyLockManager.acquireSharedLock(iKey);
    try {
      acquireSharedLock();
      try {
        return indexEngine.get(iKey);
      } finally {
        releaseSharedLock();
      }
    } finally {
      if (!txIsActive)
        keyLockManager.releaseSharedLock(iKey);
    }

  }

  public long count(Object iKey) {
    iKey = getCollatingValue(iKey);

    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();
    if (!txIsActive)
      keyLockManager.acquireSharedLock(iKey);

    try {
      acquireSharedLock();
      try {
        return indexEngine.contains(iKey) ? 1 : 0;
      } finally {
        releaseSharedLock();
      }
    } finally {
      if (!txIsActive)
        keyLockManager.releaseSharedLock(iKey);
    }
  }

  @Override
  public ODocument checkEntry(final OIdentifiable record, Object key) {
    key = getCollatingValue(key);

    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();

    if (!txIsActive)
      keyLockManager.acquireSharedLock(key);
    try {
      // CHECK IF ALREADY EXIST
      final OIdentifiable indexedRID = get(key);
      if (indexedRID != null && !indexedRID.getIdentity().equals(record.getIdentity())) {
        final Boolean mergeSameKey = metadata != null && (Boolean) metadata.field(OIndex.MERGE_KEYS);
        if (mergeSameKey != null && mergeSameKey)
          return (ODocument) indexedRID.getRecord();
        else
          throw new OIndexException("Cannot index record : " + record + " found duplicated key '" + key + "' in index " + getName()
              + " previously assigned to the record " + indexedRID);
      }
      return null;
    } finally {
      if (!txIsActive)
        keyLockManager.releaseSharedLock(key);
    }
  }

  public OIndexOneValue create(final String name, final OIndexDefinition indexDefinition, final String clusterIndexName,
      final Set<String> clustersToIndex, boolean rebuild, final OProgressListener progressListener) {
    return (OIndexOneValue) super.create(indexDefinition, clusterIndexName, clustersToIndex, rebuild, progressListener,
        determineValueSerializer());
  }

  @Override
  public OIndexCursor iterateEntries(Collection<?> keys, boolean ascSortOrder) {
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
    toKey = getCollatingValue(toKey);
    acquireSharedLock();
    try {
      return indexEngine.iterateEntriesMinor(toKey, toInclusive, ascOrder, null);
    } finally {
      releaseSharedLock();
    }
  }

  public long getSize() {
    acquireSharedLock();
    try {
      return indexEngine.size(null);
    } finally {
      releaseSharedLock();
    }
  }

  public long getKeySize() {
    acquireSharedLock();
    try {
      return indexEngine.size(null);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor cursor() {
    acquireSharedLock();
    try {
      return indexEngine.cursor(null);
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor descCursor() {
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

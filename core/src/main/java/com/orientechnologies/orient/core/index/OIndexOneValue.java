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
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerRID;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

import java.util.*;

/**
 * Abstract Index implementation that allows only one value for a key.
 *
 * @author Luca Garulli
 */
public abstract class OIndexOneValue extends OIndexAbstract<OIdentifiable> {
  public OIndexOneValue(String name, final String type, String algorithm, int version, OAbstractPaginatedStorage storage,
      String valueContainerAlgorithm, ODocument metadata) {
    super(name, type, algorithm, valueContainerAlgorithm, metadata, version, storage);
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
        while (true)
          try {
            return (OIdentifiable) storage.getIndexValue(indexId, iKey);
          } catch (OInvalidIndexEngineIdException e) {
            doReloadIndexEngine();
          }
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
        while (true)
          try {
            return storage.indexContainsKey(indexId, iKey) ? 1 : 0;
          } catch (OInvalidIndexEngineIdException e) {
            doReloadIndexEngine();
          }
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
          throw new ORecordDuplicatedException(String
              .format("Cannot index record %s: found duplicated key '%s' in index '%s' previously assigned to the record %s",
                  record, key, getName(), indexedRID), getName(), indexedRID.getIdentity());
      }
      return null;
    } finally {
      if (!txIsActive)
        keyLockManager.releaseSharedLock(key);
    }
  }

  public OIndexOneValue create(final String name, final OIndexDefinition indexDefinition, final String clusterIndexName,
      final Set<String> clustersToIndex, boolean rebuild, final OProgressListener progressListener) {
    return (OIndexOneValue) super
        .create(indexDefinition, clusterIndexName, clustersToIndex, rebuild, progressListener, determineValueSerializer());
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
            while (true)
              try {
                result = (OIdentifiable) storage.getIndexValue(indexId, key);
                break;
              } catch (OInvalidIndexEngineIdException e) {
                doReloadIndexEngine();
              }
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
      while (true)
        try {
          return storage.iterateIndexEntriesBetween(indexId, fromKey, fromInclusive, toKey, toInclusive, ascOrder, null);
        } catch (OInvalidIndexEngineIdException e) {
          doReloadIndexEngine();
        }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    fromKey = getCollatingValue(fromKey);
    acquireSharedLock();
    try {
      while (true)
        try {
          return storage.iterateIndexEntriesMajor(indexId, fromKey, fromInclusive, ascOrder, null);
        } catch (OInvalidIndexEngineIdException e) {
          doReloadIndexEngine();
        }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    toKey = getCollatingValue(toKey);
    acquireSharedLock();
    try {
      while (true) {
        try {
          return storage.iterateIndexEntriesMinor(indexId, toKey, toInclusive, ascOrder, null);
        } catch (OInvalidIndexEngineIdException e) {
          doReloadIndexEngine();
        }
      }

    } finally {
      releaseSharedLock();
    }
  }

  public long getSize() {
    acquireSharedLock();
    try {
      while (true) {
        try {
          return storage.getIndexSize(indexId, null);
        } catch (OInvalidIndexEngineIdException e) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
  }

  public long getKeySize() {
    acquireSharedLock();
    try {
      while (true) {
        try {
          return storage.getIndexSize(indexId, null);
        } catch (OInvalidIndexEngineIdException e) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor cursor() {
    acquireSharedLock();
    try {
      while (true) {
        try {
          return storage.getIndexCursor(indexId, null);
        } catch (OInvalidIndexEngineIdException e) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public OIndexCursor descCursor() {
    acquireSharedLock();
    try {
      while (true) {
        try {
          return storage.getIndexDescCursor(indexId, null);
        } catch (OInvalidIndexEngineIdException e) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public boolean isUnique() {
    return true;
  }

  @Override
  protected OBinarySerializer determineValueSerializer() {
    return OStreamSerializerRID.INSTANCE;
  }
}

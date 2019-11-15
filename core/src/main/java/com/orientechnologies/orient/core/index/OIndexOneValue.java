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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerRID;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

import java.util.*;
import java.util.function.Consumer;

/**
 * Abstract Index implementation that allows only one value for a key.
 *
 * @author Luca Garulli
 */
public abstract class OIndexOneValue extends OIndexAbstract<OIdentifiable> {
  public OIndexOneValue(String name, final String type, String algorithm, int version, OAbstractPaginatedStorage storage,
      String valueContainerAlgorithm, ODocument metadata, final int binaryFormatVersion) {
    super(name, type, algorithm, valueContainerAlgorithm, metadata, version, storage, binaryFormatVersion);
  }

  public OIdentifiable get(Object iKey) {
    iKey = getCollatingValue(iKey);

    acquireSharedLock();
    try {
      while (true)
        try {
          return OIndexInternal.securityFilterOnRead(this, (OIdentifiable) storage.getIndexValue(indexId, iKey));
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
    } finally {
      releaseSharedLock();
    }
  }

  public long count(Object iKey) {
    iKey = getCollatingValue(iKey);

    acquireSharedLock();
    try {
      return get(iKey) == null ? 0 : 1;
    } finally {
      releaseSharedLock();
    }
  }

  public OIndexOneValue create(final String name, final OIndexDefinition indexDefinition, final String clusterIndexName,
      final Set<String> clustersToIndex, boolean rebuild, final OProgressListener progressListener) {
    return (OIndexOneValue) super
        .create(indexDefinition, clusterIndexName, clustersToIndex, rebuild, progressListener, determineValueSerializer());
  }

  @Override
  public IndexCursor iterateEntries(Collection<?> keys, boolean ascSortOrder) {
    final List<Object> sortedKeys = new ArrayList<>(keys);
    final Comparator<Object> comparator;

    if (ascSortOrder)
      comparator = ODefaultComparator.INSTANCE;
    else
      comparator = Collections.reverseOrder(ODefaultComparator.INSTANCE);

    sortedKeys.sort(comparator);

    return new IndexCursorSecurityDecorator(new IndexCursor() {
      private final Iterator<?> keysIterator = sortedKeys.iterator();

      @Override
      public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
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
              } catch (OInvalidIndexEngineIdException ignore) {
                doReloadIndexEngine();
              }
          } finally {
            releaseSharedLock();
          }
        }

        if (result == null) {
          return false;
        }

        action.accept(new ORawPair<>(key, result.getIdentity()));
        return true;
      }

      @Override
      public Spliterator<ORawPair<Object, ORID>> trySplit() {
        return null;
      }

      @Override
      public long estimateSize() {
        return Long.MAX_VALUE;
      }

      @Override
      public int characteristics() {
        return NONNULL | ORDERED;
      }
    }, this);
  }

  @Override
  public IndexCursor iterateEntriesBetween(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive,
      boolean ascOrder) {
    fromKey = getCollatingValue(fromKey);
    toKey = getCollatingValue(toKey);

    acquireSharedLock();
    try {
      while (true)
        try {
          return new IndexCursorSecurityDecorator(
              storage.iterateIndexEntriesBetween(indexId, fromKey, fromInclusive, toKey, toInclusive, ascOrder, null), this);
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public IndexCursor iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    fromKey = getCollatingValue(fromKey);
    acquireSharedLock();
    try {
      while (true)
        try {
          return new IndexCursorSecurityDecorator(storage.iterateIndexEntriesMajor(indexId, fromKey, fromInclusive, ascOrder, null),
              this);
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public IndexCursor iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    toKey = getCollatingValue(toKey);
    acquireSharedLock();
    try {
      while (true) {
        try {
          return new IndexCursorSecurityDecorator(storage.iterateIndexEntriesMinor(indexId, toKey, toInclusive, ascOrder, null),
              this);
        } catch (OInvalidIndexEngineIdException ignore) {
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
        } catch (OInvalidIndexEngineIdException ignore) {
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
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public IndexCursor cursor() {
    acquireSharedLock();
    try {
      while (true) {
        try {
          return new IndexCursorSecurityDecorator(storage.getIndexCursor(indexId, null), this);
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public IndexCursor descCursor() {
    acquireSharedLock();
    try {
      while (true) {
        try {
          return new IndexCursorSecurityDecorator(storage.getIndexDescCursor(indexId, null), this);
        } catch (OInvalidIndexEngineIdException ignore) {
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

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
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerRID;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Abstract Index implementation that allows only one value for a key.
 *
 * @author Luca Garulli
 */
public abstract class OIndexOneValue extends OIndexAbstract {

  public OIndexOneValue(
      String name,
      final String type,
      String algorithm,
      int version,
      OAbstractPaginatedStorage storage,
      String valueContainerAlgorithm,
      ODocument metadata,
      final int binaryFormatVersion,
      OAtomicOperationsManager atomicOperationsManager) {
    super(
        name,
        type,
        algorithm,
        valueContainerAlgorithm,
        metadata,
        version,
        storage,
        binaryFormatVersion,
        atomicOperationsManager);
  }

  @Deprecated
  @Override
  public Object get(Object key) {
    final Iterator<ORID> iterator;
    try (Stream<ORID> stream = getRids(key)) {
      iterator = stream.iterator();
      if (iterator.hasNext()) {
        return iterator.next();
      }
    }

    return null;
  }

  @Override
  public Stream<ORID> getRids(Object key) {
    key = getCollatingValue(key);

    acquireSharedLock();
    try {
      while (true)
        try {
          final Stream<ORID> stream;
          if (apiVersion == 0) {
            final ORID rid = (ORID) storage.getIndexValue(indexId, key);
            if (rid == null) {
              return Stream.empty();
            }
            //noinspection resource
            stream = Stream.of(rid);
          } else if (apiVersion == 1) {
            //noinspection resource
            stream = storage.getIndexValues(indexId, key);
          } else {
            throw new IllegalStateException("Unknown version of index API " + apiVersion);
          }
          return IndexStreamSecurityDecorator.decorateRidStream(this, stream);
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
    } finally {
      releaseSharedLock();
    }
  }

  public OIndexOneValue create(
      final String name,
      final OIndexDefinition indexDefinition,
      final String clusterIndexName,
      final Set<String> clustersToIndex,
      boolean rebuild,
      final OProgressListener progressListener) {
    return (OIndexOneValue)
        super.create(
            indexDefinition,
            clusterIndexName,
            clustersToIndex,
            rebuild,
            progressListener,
            determineValueSerializer());
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntries(Collection<?> keys, boolean ascSortOrder) {
    final List<Object> sortedKeys = new ArrayList<>(keys);
    final Comparator<Object> comparator;

    if (ascSortOrder) comparator = ODefaultComparator.INSTANCE;
    else comparator = Collections.reverseOrder(ODefaultComparator.INSTANCE);

    sortedKeys.sort(comparator);

    //noinspection resource
    return IndexStreamSecurityDecorator.decorateStream(
        this,
        sortedKeys.stream()
            .flatMap(
                (key) -> {
                  final Object collatedKey = getCollatingValue(key);

                  acquireSharedLock();
                  try {
                    while (true) {
                      try {
                        if (apiVersion == 0) {
                          final ORID rid = (ORID) storage.getIndexValue(indexId, collatedKey);
                          if (rid == null) {
                            return Stream.empty();
                          }
                          return Stream.of(new ORawPair<>(collatedKey, rid));
                        } else if (apiVersion == 1) {
                          //noinspection resource
                          return storage
                              .getIndexValues(indexId, collatedKey)
                              .map((rid) -> new ORawPair<>(collatedKey, rid));
                        } else {
                          throw new IllegalStateException(
                              "Invalid version of index API - " + apiVersion);
                        }
                      } catch (OInvalidIndexEngineIdException ignore) {
                        doReloadIndexEngine();
                      }
                    }
                  } finally {
                    releaseSharedLock();
                  }
                })
            .filter(Objects::nonNull));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesBetween(
      Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive, boolean ascOrder) {
    fromKey = getCollatingValue(fromKey);
    toKey = getCollatingValue(toKey);

    acquireSharedLock();
    try {
      while (true)
        try {
          return IndexStreamSecurityDecorator.decorateStream(
              this,
              storage.iterateIndexEntriesBetween(
                  indexId, fromKey, fromInclusive, toKey, toInclusive, ascOrder, null));
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesMajor(
      Object fromKey, boolean fromInclusive, boolean ascOrder) {
    fromKey = getCollatingValue(fromKey);
    acquireSharedLock();
    try {
      while (true)
        try {
          return IndexStreamSecurityDecorator.decorateStream(
              this,
              storage.iterateIndexEntriesMajor(indexId, fromKey, fromInclusive, ascOrder, null));
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesMinor(
      Object toKey, boolean toInclusive, boolean ascOrder) {
    toKey = getCollatingValue(toKey);
    acquireSharedLock();
    try {
      while (true) {
        try {
          return IndexStreamSecurityDecorator.decorateStream(
              this, storage.iterateIndexEntriesMinor(indexId, toKey, toInclusive, ascOrder, null));
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }

    } finally {
      releaseSharedLock();
    }
  }

  public long size() {
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
  public Stream<ORawPair<Object, ORID>> stream() {
    acquireSharedLock();
    try {
      while (true) {
        try {
          return IndexStreamSecurityDecorator.decorateStream(
              this, storage.getIndexStream(indexId, null));
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
  }

  @Override
  public Stream<ORawPair<Object, ORID>> descStream() {
    acquireSharedLock();
    try {
      while (true) {
        try {
          return IndexStreamSecurityDecorator.decorateStream(
              this, storage.getIndexDescStream(indexId, null));
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

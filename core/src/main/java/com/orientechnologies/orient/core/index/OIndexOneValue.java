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
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.stream.Streams;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerRID;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
      final int binaryFormatVersion) {
    super(
        name,
        type,
        algorithm,
        valueContainerAlgorithm,
        metadata,
        version,
        storage,
        binaryFormatVersion);
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
    Stream<ORID> stream;
    try {
      while (true)
        try {
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
          stream = IndexStreamSecurityDecorator.decorateRidStream(this, stream);
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
    } finally {
      releaseSharedLock();
    }
    ODatabaseDocumentInternal database = getDatabase();
    final OTransactionIndexChanges indexChanges =
        database.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    ORID rid;
    if (!indexChanges.cleared) {
      // BEGIN FROM THE UNDERLYING RESULT SET
      //noinspection resource
      rid = stream.findFirst().orElse(null);
    } else {
      rid = null;
    }

    final ORawPair<Object, ORID> txIndexEntry = calculateTxIndexEntry(key, rid, indexChanges);
    if (txIndexEntry == null) {
      return Stream.empty();
    }

    return IndexStreamSecurityDecorator.decorateRidStream(this, Stream.of(txIndexEntry.second));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntries(Collection<?> keys, boolean ascSortOrder) {
    final List<Object> sortedKeys = new ArrayList<>(keys);
    final Comparator<Object> comparator;

    if (ascSortOrder) comparator = ODefaultComparator.INSTANCE;
    else comparator = Collections.reverseOrder(ODefaultComparator.INSTANCE);

    sortedKeys.sort(comparator);

    //noinspection resource
    Stream<ORawPair<Object, ORID>> stream =
        IndexStreamSecurityDecorator.decorateStream(
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
    ODatabaseDocumentInternal database = getDatabase();
    final OTransactionIndexChanges indexChanges =
        database.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<Object, ORID>> txStream =
        keys.stream()
            .map((key) -> calculateTxIndexEntry(getCollatingValue(key), null, indexChanges))
            .filter(Objects::nonNull)
            .sorted(
                (entryOne, entryTwo) -> {
                  if (ascSortOrder) {
                    return ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
                  } else {
                    return -ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
                  }
                });

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, ascSortOrder));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesBetween(
      Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive, boolean ascOrder) {
    fromKey = getCollatingValue(fromKey);
    toKey = getCollatingValue(toKey);
    Stream<ORawPair<Object, ORID>> stream;
    acquireSharedLock();
    try {
      while (true)
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this,
                  storage.iterateIndexEntriesBetween(
                      indexId, fromKey, fromInclusive, toKey, toInclusive, ascOrder, null));
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
    } finally {
      releaseSharedLock();
    }
    ODatabaseDocumentInternal database = getDatabase();
    final OTransactionIndexChanges indexChanges =
        database.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    final Stream<ORawPair<Object, ORID>> txStream;
    if (ascOrder) {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxBetweenIndexForwardSpliterator(
                  this, fromKey, fromInclusive, toKey, toInclusive, indexChanges),
              false);
    } else {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxBetweenIndexBackwardSpliterator(
                  this, fromKey, fromInclusive, toKey, toInclusive, indexChanges),
              false);
    }

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, ascOrder));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesMajor(
      Object fromKey, boolean fromInclusive, boolean ascOrder) {
    fromKey = getCollatingValue(fromKey);
    Stream<ORawPair<Object, ORID>> stream;
    acquireSharedLock();
    try {
      while (true)
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this,
                  storage.iterateIndexEntriesMajor(
                      indexId, fromKey, fromInclusive, ascOrder, null));
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
    } finally {
      releaseSharedLock();
    }
    ODatabaseDocumentInternal database = getDatabase();
    final OTransactionIndexChanges indexChanges =
        database.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    fromKey = getCollatingValue(fromKey);

    final Stream<ORawPair<Object, ORID>> txStream;

    final Object lastKey = indexChanges.getLastKey();
    if (ascOrder) {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxBetweenIndexForwardSpliterator(
                  this, fromKey, fromInclusive, lastKey, true, indexChanges),
              false);
    } else {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxBetweenIndexBackwardSpliterator(
                  this, fromKey, fromInclusive, lastKey, true, indexChanges),
              false);
    }

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, ascOrder));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesMinor(
      Object toKey, boolean toInclusive, boolean ascOrder) {
    toKey = getCollatingValue(toKey);
    Stream<ORawPair<Object, ORID>> stream;
    acquireSharedLock();
    try {
      while (true) {
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this,
                  storage.iterateIndexEntriesMinor(indexId, toKey, toInclusive, ascOrder, null));
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }

    } finally {
      releaseSharedLock();
    }
    ODatabaseDocumentInternal database = getDatabase();
    final OTransactionIndexChanges indexChanges =
        database.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    toKey = getCollatingValue(toKey);

    final Stream<ORawPair<Object, ORID>> txStream;

    final Object firstKey = indexChanges.getFirstKey();
    if (ascOrder) {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxBetweenIndexForwardSpliterator(
                  this, firstKey, true, toKey, toInclusive, indexChanges),
              false);
    } else {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxBetweenIndexBackwardSpliterator(
                  this, firstKey, true, toKey, toInclusive, indexChanges),
              false);
    }

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, ascOrder));
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
    Stream<ORawPair<Object, ORID>> stream;
    acquireSharedLock();
    try {
      while (true) {
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this, storage.getIndexStream(indexId, null));
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
    ODatabaseDocumentInternal database = getDatabase();
    final OTransactionIndexChanges indexChanges =
        database.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    final Stream<ORawPair<Object, ORID>> txStream =
        StreamSupport.stream(
            new PureTxBetweenIndexForwardSpliterator(this, null, true, null, true, indexChanges),
            false);
    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, true));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> descStream() {
    Stream<ORawPair<Object, ORID>> stream;
    acquireSharedLock();
    try {
      while (true) {
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this, storage.getIndexDescStream(indexId, null));
          break;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }
    } finally {
      releaseSharedLock();
    }
    ODatabaseDocumentInternal database = getDatabase();
    final OTransactionIndexChanges indexChanges =
        database.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    final Stream<ORawPair<Object, ORID>> txStream =
        StreamSupport.stream(
            new PureTxBetweenIndexBackwardSpliterator(this, null, true, null, true, indexChanges),
            false);
    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, false));
  }

  @Override
  public boolean isUnique() {
    return true;
  }

  @Override
  protected OBinarySerializer determineValueSerializer() {
    return OStreamSerializerRID.INSTANCE;
  }

  ORawPair<Object, ORID> calculateTxIndexEntry(
      Object key, final ORID backendValue, final OTransactionIndexChanges indexChanges) {
    key = getCollatingValue(key);
    ORID result = backendValue;
    final OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerKey(key);
    if (changesPerKey.isEmpty()) {
      if (backendValue == null) {
        return null;
      } else {
        return new ORawPair<>(key, backendValue);
      }
    }

    for (OTransactionIndexEntry entry : changesPerKey.getEntriesAsList()) {
      if (entry.getOperation() == OPERATION.REMOVE) result = null;
      else if (entry.getOperation() == OPERATION.PUT) result = entry.getValue().getIdentity();
    }

    if (result == null) {
      return null;
    }

    return new ORawPair<>(key, result);
  }

  private Stream<ORawPair<Object, ORID>> mergeTxAndBackedStreams(
      OTransactionIndexChanges indexChanges,
      Stream<ORawPair<Object, ORID>> txStream,
      Stream<ORawPair<Object, ORID>> backedStream,
      boolean ascSortOrder) {
    return Streams.mergeSortedSpliterators(
        txStream,
        backedStream
            .map(
                (entry) ->
                    calculateTxIndexEntry(
                        getCollatingValue(entry.first), entry.second, indexChanges))
            .filter(Objects::nonNull),
        (entryOne, entryTwo) -> {
          if (ascSortOrder) {
            return ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
          } else {
            return -ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
          }
        });
  }

  @Override
  public OIndexOneValue put(Object key, final OIdentifiable value) {
    final ORID rid = value.getIdentity();

    if (!rid.isValid()) {
      if (value instanceof ORecord) {
        // EARLY SAVE IT
        ((ORecord) value).save();
      } else {
        throw new IllegalArgumentException(
            "Cannot store non persistent RID as index value for key '" + key + "'");
      }
    }
    key = getCollatingValue(key);

    ODatabaseDocumentInternal database = getDatabase();
    if (database.getTransaction().isActive()) {
      OTransaction singleTx = database.getTransaction();
      singleTx.addIndexEntry(
          this, super.getName(), OTransactionIndexChanges.OPERATION.PUT, key, value.getIdentity());

    } else {
      database.begin();
      OTransaction singleTx = database.getTransaction();
      singleTx.addIndexEntry(
          this, super.getName(), OTransactionIndexChanges.OPERATION.PUT, key, value.getIdentity());
      database.commit();
    }
    return this;
  }
}

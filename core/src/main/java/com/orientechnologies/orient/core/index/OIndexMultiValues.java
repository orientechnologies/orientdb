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
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OMixedIndexRIDContainerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerSBTreeIndexRIDContainer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OIndexRIDContainerSBTree;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OMixedIndexRIDContainer;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Abstract index implementation that supports multi-values for the same key.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OIndexMultiValues extends OIndexAbstract {

  OIndexMultiValues(
      String name,
      final String type,
      String algorithm,
      int version,
      OAbstractPaginatedStorage storage,
      String valueContainerAlgorithm,
      final ODocument metadata,
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
  public Collection<ORID> get(Object key) {
    final List<ORID> rids;
    try (Stream<ORID> stream = getRids(key)) {
      rids = stream.collect(Collectors.toList());
    }
    return rids;
  }

  @Override
  public Stream<ORID> getRids(Object key) {
    final Object collatedKey = getCollatingValue(key);
    Stream<ORID> backedStream;
    acquireSharedLock();
    try {
      Stream<ORID> stream;
      while (true) {
        try {
          if (apiVersion == 0) {
            //noinspection unchecked
            final Collection<ORID> values =
                (Collection<ORID>) storage.getIndexValue(indexId, collatedKey);
            if (values != null) {
              //noinspection resource
              stream = values.stream();
            } else {
              //noinspection resource
              stream = Stream.empty();
            }
          } else if (apiVersion == 1) {
            //noinspection resource
            stream = storage.getIndexValues(indexId, collatedKey);
          } else {
            throw new IllegalStateException("Invalid version of index API - " + apiVersion);
          }
          backedStream = IndexStreamSecurityDecorator.decorateRidStream(this, stream);
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
      return backedStream;
    }
    Set<OIdentifiable> txChanges = calculateTxValue(collatedKey, indexChanges);
    if (txChanges == null) {
      txChanges = Collections.emptySet();
    }
    return IndexStreamSecurityDecorator.decorateRidStream(
        this,
        Stream.concat(
            backedStream
                .map((rid) -> calculateTxIndexEntry(collatedKey, rid, indexChanges))
                .filter(Objects::nonNull)
                .map((pair) -> pair.second),
            txChanges.stream().map(OIdentifiable::getIdentity)));
  }

  public OIndexMultiValues put(Object key, final OIdentifiable singleValue) {
    final ORID rid = singleValue.getIdentity();

    if (!rid.isValid()) {
      if (singleValue instanceof ORecord) {
        // EARLY SAVE IT
        ((ORecord) singleValue).save();
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
          this, super.getName(), OTransactionIndexChanges.OPERATION.PUT, key, singleValue);
    } else {
      database.begin();
      OTransaction singleTx = database.getTransaction();
      singleTx.addIndexEntry(
          this, super.getName(), OTransactionIndexChanges.OPERATION.PUT, key, singleValue);
      database.commit();
    }
    return this;
  }

  @Override
  public void doPut(OAbstractPaginatedStorage storage, Object key, ORID rid)
      throws OInvalidIndexEngineIdException {
    if (apiVersion == 0) {
      doPutV0(indexId, storage, binaryFormatVersion, valueContainerAlgorithm, getName(), key, rid);
    } else if (apiVersion == 1) {
      doPutV1(storage, indexId, key, rid);
    } else {
      throw new IllegalStateException("Invalid API version, " + apiVersion);
    }
  }

  @Override
  public boolean isNativeTxSupported() {
    return true;
  }

  private static void doPutV0(
      final int indexId,
      final OAbstractPaginatedStorage storage,
      final int binaryFormatVersion,
      String valueContainerAlgorithm,
      String indexName,
      Object key,
      ORID identity)
      throws OInvalidIndexEngineIdException {
    final OIndexKeyUpdater<Object> creator =
        (oldValue, bonsayFileId) -> {
          @SuppressWarnings("unchecked")
          Set<OIdentifiable> toUpdate = (Set<OIdentifiable>) oldValue;
          if (toUpdate == null) {
            if (ODefaultIndexFactory.SBTREE_BONSAI_VALUE_CONTAINER.equals(
                valueContainerAlgorithm)) {
              if (binaryFormatVersion >= 13) {
                toUpdate = new OMixedIndexRIDContainer(indexName, bonsayFileId);
              } else {
                toUpdate = new OIndexRIDContainer(indexName, true, bonsayFileId);
              }
            } else {
              throw new IllegalStateException("MVRBTree is not supported any more");
            }
          }
          if (toUpdate instanceof OIndexRIDContainer) {
            boolean isTree = !((OIndexRIDContainer) toUpdate).isEmbedded();
            toUpdate.add(identity);

            if (isTree) {
              //noinspection unchecked
              return OIndexUpdateAction.nothing();
            } else {
              return OIndexUpdateAction.changed(toUpdate);
            }
          } else if (toUpdate instanceof OMixedIndexRIDContainer) {
            final OMixedIndexRIDContainer ridContainer = (OMixedIndexRIDContainer) toUpdate;
            final boolean embeddedWasUpdated = ridContainer.addEntry(identity);

            if (!embeddedWasUpdated) {
              //noinspection unchecked
              return OIndexUpdateAction.nothing();
            } else {
              return OIndexUpdateAction.changed(toUpdate);
            }
          } else {
            if (toUpdate.add(identity)) {
              return OIndexUpdateAction.changed(toUpdate);
            } else {
              //noinspection unchecked
              return OIndexUpdateAction.nothing();
            }
          }
        };

    storage.updateIndexEntry(indexId, key, creator);
  }

  private static void doPutV1(
      OAbstractPaginatedStorage storage, int indexId, Object key, ORID identity)
      throws OInvalidIndexEngineIdException {
    storage.putRidIndexEntry(indexId, key, identity);
  }

  @Override
  public boolean remove(Object key, final OIdentifiable value) {
    key = getCollatingValue(key);

    ODatabaseDocumentInternal database = getDatabase();
    if (database.getTransaction().isActive()) {
      database.getTransaction().addIndexEntry(this, super.getName(), OPERATION.REMOVE, key, value);
    } else {
      database.begin();
      database.getTransaction().addIndexEntry(this, super.getName(), OPERATION.REMOVE, key, value);
      database.commit();
    }
    return true;
  }

  @Override
  public boolean doRemove(OAbstractPaginatedStorage storage, Object key, ORID rid)
      throws OInvalidIndexEngineIdException {
    if (apiVersion == 0) {
      return doRemoveV0(indexId, storage, key, rid);
    }

    if (apiVersion == 1) {
      return doRemoveV1(indexId, storage, key, rid);
    }

    throw new IllegalStateException("Invalid API version, " + apiVersion);
  }

  private static boolean doRemoveV0(
      int indexId, OAbstractPaginatedStorage storage, Object key, OIdentifiable value)
      throws OInvalidIndexEngineIdException {
    Set<OIdentifiable> values;
    //noinspection unchecked
    values = (Set<OIdentifiable>) storage.getIndexValue(indexId, key);

    if (values == null) {
      return false;
    }

    final OModifiableBoolean removed = new OModifiableBoolean(false);

    final OIndexKeyUpdater<Object> creator = new EntityRemover(value, removed);

    storage.updateIndexEntry(indexId, key, creator);

    return removed.getValue();
  }

  private static boolean doRemoveV1(
      int indexId, OAbstractPaginatedStorage storage, Object key, OIdentifiable value)
      throws OInvalidIndexEngineIdException {
    return storage.removeRidIndexEntry(indexId, key, value.getIdentity());
  }

  protected OBinarySerializer<?> determineValueSerializer() {
    if (binaryFormatVersion >= 13) {
      return storage
          .getComponentsFactory()
          .binarySerializerFactory
          .getObjectSerializer(OMixedIndexRIDContainerSerializer.ID);
    }

    return storage
        .getComponentsFactory()
        .binarySerializerFactory
        .getObjectSerializer(OStreamSerializerSBTreeIndexRIDContainer.ID);
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesBetween(
      Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive, boolean ascOrder) {
    fromKey = getCollatingValue(fromKey);
    toKey = getCollatingValue(toKey);
    Stream<ORawPair<Object, ORID>> stream;
    acquireSharedLock();
    try {
      while (true) {
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this,
                  storage.iterateIndexEntriesBetween(
                      indexId,
                      fromKey,
                      fromInclusive,
                      toKey,
                      toInclusive,
                      ascOrder,
                      MultiValuesTransformer.INSTANCE));
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

    final Stream<ORawPair<Object, ORID>> txStream;
    if (ascOrder) {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxMultiValueBetweenIndexForwardSpliterator(
                  this, fromKey, fromInclusive, toKey, toInclusive, indexChanges),
              false);
    } else {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxMultiValueBetweenIndexBackwardSplititerator(
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
      while (true) {
        try {
          stream =
              IndexStreamSecurityDecorator.decorateStream(
                  this,
                  storage.iterateIndexEntriesMajor(
                      indexId, fromKey, fromInclusive, ascOrder, MultiValuesTransformer.INSTANCE));
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

    final Stream<ORawPair<Object, ORID>> txStream;

    final Object lastKey = indexChanges.getLastKey();
    if (ascOrder) {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxMultiValueBetweenIndexForwardSpliterator(
                  this, fromKey, fromInclusive, lastKey, true, indexChanges),
              false);
    } else {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxMultiValueBetweenIndexBackwardSplititerator(
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
                  storage.iterateIndexEntriesMinor(
                      indexId, toKey, toInclusive, ascOrder, MultiValuesTransformer.INSTANCE));
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

    final Stream<ORawPair<Object, ORID>> txStream;

    final Object firstKey = indexChanges.getFirstKey();
    if (ascOrder) {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxMultiValueBetweenIndexForwardSpliterator(
                  this, firstKey, true, toKey, toInclusive, indexChanges),
              false);
    } else {
      //noinspection resource
      txStream =
          StreamSupport.stream(
              new PureTxMultiValueBetweenIndexBackwardSplititerator(
                  this, firstKey, true, toKey, toInclusive, indexChanges),
              false);
    }

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, ascOrder));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntries(Collection<?> keys, boolean ascSortOrder) {
    final List<Object> sortedKeys = new ArrayList<>(keys);
    final Comparator<Object> comparator;
    if (ascSortOrder) {
      comparator = ODefaultComparator.INSTANCE;
    } else {
      comparator = Collections.reverseOrder(ODefaultComparator.INSTANCE);
    }

    sortedKeys.sort(comparator);

    //noinspection resource
    Stream<ORawPair<Object, ORID>> stream =
        IndexStreamSecurityDecorator.decorateStream(
            this,
            sortedKeys.stream()
                .flatMap(
                    (key) -> {
                      key = getCollatingValue(key);

                      final Object entryKey = key;
                      acquireSharedLock();
                      try {
                        while (true) {
                          try {
                            if (apiVersion == 0) {
                              //noinspection unchecked,resource
                              return Optional.ofNullable(
                                      (Collection<ORID>) storage.getIndexValue(indexId, key))
                                  .map(
                                      (rids) ->
                                          rids.stream().map((rid) -> new ORawPair<>(entryKey, rid)))
                                  .orElse(Stream.empty());
                            } else if (apiVersion == 1) {
                              //noinspection resource
                              return storage
                                  .getIndexValues(indexId, key)
                                  .map((rid) -> new ORawPair<>(entryKey, rid));
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
                    }));

    ODatabaseDocumentInternal database = getDatabase();
    final OTransactionIndexChanges indexChanges =
        database.getTransaction().getIndexChangesInternal(getName());
    if (indexChanges == null) {
      return stream;
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<Object, ORID>> txStream =
        keys.stream()
            .flatMap(
                (key) -> {
                  final Set<OIdentifiable> result =
                      calculateTxValue(getCollatingValue(key), indexChanges);
                  if (result != null) {
                    return result.stream()
                        .map((rid) -> new ORawPair<>(getCollatingValue(key), rid.getIdentity()));
                  }
                  return null;
                })
            .filter(Objects::nonNull)
            .sorted(
                (entryOne, entryTwo) -> {
                  if (ascSortOrder) {
                    return ODefaultComparator.INSTANCE.compare(
                        getCollatingValue(entryOne.first), getCollatingValue(entryTwo.first));
                  } else {
                    return -ODefaultComparator.INSTANCE.compare(
                        getCollatingValue(entryOne.first), getCollatingValue(entryTwo.first));
                  }
                });

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, ascSortOrder));
  }

  static Set<OIdentifiable> calculateTxValue(
      final Object key, OTransactionIndexChanges indexChanges) {
    final List<OIdentifiable> result = new ArrayList<>();
    final OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerKey(key);
    if (changesPerKey.isEmpty()) {
      return null;
    }

    for (OTransactionIndexEntry entry : changesPerKey.getEntriesAsList()) {
      if (entry.getOperation() == OPERATION.REMOVE) {
        if (entry.getValue() == null) result.clear();
        else result.remove(entry.getValue());
      } else result.add(entry.getValue());
    }

    if (result.isEmpty()) return null;

    return new HashSet<>(result);
  }

  public long size() {
    acquireSharedLock();
    long tot;
    try {
      while (true) {
        try {
          tot = storage.getIndexSize(indexId, MultiValuesTransformer.INSTANCE);
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
        database.getTransaction().getIndexChanges(getName());
    if (indexChanges != null) {
      try (Stream<ORawPair<Object, ORID>> stream = stream()) {
        return stream.count();
      }
    }

    return tot;
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
                  this, storage.getIndexStream(indexId, MultiValuesTransformer.INSTANCE));
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
            new PureTxMultiValueBetweenIndexForwardSpliterator(
                this, null, true, null, true, indexChanges),
            false);

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, true));
  }

  private Stream<ORawPair<Object, ORID>> mergeTxAndBackedStreams(
      OTransactionIndexChanges indexChanges,
      Stream<ORawPair<Object, ORID>> txStream,
      Stream<ORawPair<Object, ORID>> backedStream,
      boolean ascOrder) {
    return Streams.mergeSortedSpliterators(
        txStream,
        backedStream
            .map((entry) -> calculateTxIndexEntry(entry.first, entry.second, indexChanges))
            .filter(Objects::nonNull),
        (entryOne, entryTwo) -> {
          if (ascOrder) {
            return ODefaultComparator.INSTANCE.compare(
                getCollatingValue(entryOne.first), getCollatingValue(entryTwo.first));
          } else {
            return -ODefaultComparator.INSTANCE.compare(
                getCollatingValue(entryOne.first), getCollatingValue(entryTwo.first));
          }
        });
  }

  private ORawPair<Object, ORID> calculateTxIndexEntry(
      Object key, final ORID backendValue, OTransactionIndexChanges indexChanges) {
    key = getCollatingValue(key);
    final OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerKey(key);
    if (changesPerKey.isEmpty()) {
      return new ORawPair<>(key, backendValue);
    }

    int putCounter = 1;
    for (OTransactionIndexEntry entry : changesPerKey.getEntriesAsList()) {
      if (entry.getOperation() == OPERATION.PUT && entry.getValue().equals(backendValue))
        putCounter++;
      else if (entry.getOperation() == OPERATION.REMOVE) {
        if (entry.getValue() == null) putCounter = 0;
        else if (entry.getValue().equals(backendValue) && putCounter > 0) putCounter--;
      }
    }

    if (putCounter <= 0) {
      return null;
    }

    return new ORawPair<>(key, backendValue);
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
                  this, storage.getIndexDescStream(indexId, MultiValuesTransformer.INSTANCE));
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
            new PureTxMultiValueBetweenIndexBackwardSplititerator(
                this, null, true, null, true, indexChanges),
            false);

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    return IndexStreamSecurityDecorator.decorateStream(
        this, mergeTxAndBackedStreams(indexChanges, txStream, stream, false));
  }

  private static final class MultiValuesTransformer implements OBaseIndexEngine.ValuesTransformer {
    private static final MultiValuesTransformer INSTANCE = new MultiValuesTransformer();

    @Override
    public Collection<ORID> transformFromValue(Object value) {
      //noinspection unchecked
      return (Collection<ORID>) value;
    }
  }

  private static class EntityRemover implements OIndexKeyUpdater<Object> {
    private final OIdentifiable value;
    private final OModifiableBoolean removed;

    private EntityRemover(OIdentifiable value, OModifiableBoolean removed) {
      this.value = value;
      this.removed = removed;
    }

    @Override
    public OIndexUpdateAction<Object> update(Object persistentValue, AtomicLong bonsayFileId) {
      @SuppressWarnings("unchecked")
      Set<OIdentifiable> values = (Set<OIdentifiable>) persistentValue;
      if (value == null) {
        removed.setValue(true);

        //noinspection unchecked
        return OIndexUpdateAction.remove();
      } else if (values.remove(value)) {
        removed.setValue(true);

        if (values.isEmpty()) {
          // remove tree ridbag too
          if (values instanceof OMixedIndexRIDContainer) {
            ((OMixedIndexRIDContainer) values).delete();
          } else if (values instanceof OIndexRIDContainerSBTree) {
            ((OIndexRIDContainerSBTree) values).delete();
          }

          //noinspection unchecked
          return OIndexUpdateAction.remove();
        } else {
          return OIndexUpdateAction.changed(values);
        }
      }

      return OIndexUpdateAction.changed(values);
    }
  }
}

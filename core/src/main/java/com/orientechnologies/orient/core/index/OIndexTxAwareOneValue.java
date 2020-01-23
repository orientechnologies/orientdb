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
import com.orientechnologies.common.spliterators.Streams;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Transactional wrapper for indexes. Stores changes locally to the transaction until tx.commit(). All the other operations are
 * delegated to the wrapped OIndex instance.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OIndexTxAwareOneValue extends OIndexTxAware<OIdentifiable> {
  private class PureTxBetweenIndexForwardSpliterator implements Spliterator<ORawPair<Object, ORID>> {
    private final OTransactionIndexChanges indexChanges;
    private       Object                   lastKey;

    private Object nextKey;

    private PureTxBetweenIndexForwardSpliterator(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive,
        OTransactionIndexChanges indexChanges) {
      this.indexChanges = indexChanges;

      if (fromKey != null) {
        fromKey = enhanceFromCompositeKeyBetweenAsc(fromKey, fromInclusive);
      }
      if (toKey != null) {
        toKey = enhanceToCompositeKeyBetweenAsc(toKey, toInclusive);
      }

      final Object[] keys = indexChanges.firstAndLastKeys(fromKey, fromInclusive, toKey, toInclusive);
      if (keys.length == 0) {
        nextKey = null;
      } else {
        Object firstKey = keys[0];
        lastKey = keys[1];

        nextKey = firstKey;
      }
    }

    @Override
    public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
      if (nextKey == null) {
        return false;
      }

      ORawPair<Object, ORID> result;

      do {
        result = calculateTxIndexEntry(nextKey, null, indexChanges);
        nextKey = indexChanges.getHigherKey(nextKey);

        if (nextKey != null && ODefaultComparator.INSTANCE.compare(nextKey, lastKey) > 0) {
          nextKey = null;
        }

      } while (result == null && nextKey != null);

      if (nextKey == null) {
        return false;
      }

      action.accept(result);
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
      return NONNULL | SORTED | ORDERED;
    }

    @Override
    public Comparator<? super ORawPair<Object, ORID>> getComparator() {
      return (entryOne, entryTwo) -> ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
    }
  }

  private class PureTxBetweenIndexBackwardSpliterator implements Spliterator<ORawPair<Object, ORID>> {
    private final OTransactionIndexChanges indexChanges;
    private       Object                   firstKey;

    private Object nextKey;

    private PureTxBetweenIndexBackwardSpliterator(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive,
        OTransactionIndexChanges indexChanges) {
      this.indexChanges = indexChanges;

      if (fromKey != null) {
        fromKey = enhanceFromCompositeKeyBetweenDesc(fromKey, fromInclusive);
      }
      if (toKey != null) {
        toKey = enhanceToCompositeKeyBetweenDesc(toKey, toInclusive);
      }

      final Object[] keys = indexChanges.firstAndLastKeys(fromKey, fromInclusive, toKey, toInclusive);
      if (keys.length == 0) {
        nextKey = null;
      } else {
        firstKey = keys[0];
        nextKey = keys[1];
      }
    }

    @Override
    public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
      if (nextKey == null) {
        return false;
      }

      ORawPair<Object, ORID> result;
      do {
        result = calculateTxIndexEntry(nextKey, null, indexChanges);
        nextKey = indexChanges.getLowerKey(nextKey);

        if (nextKey != null && ODefaultComparator.INSTANCE.compare(nextKey, firstKey) < 0)
          nextKey = null;
      } while (result == null && nextKey != null);

      if (nextKey == null) {
        return false;
      }

      action.accept(result);
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
      return NONNULL | SORTED | ORDERED;
    }

    @Override
    public Comparator<? super ORawPair<Object, ORID>> getComparator() {
      return (entryOne, entryTwo) -> -ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
    }
  }

  public OIndexTxAwareOneValue(final ODatabaseDocumentInternal database, final OIndexInternal delegate) {
    super(database, delegate);
  }

  @Deprecated
  @Override
  public OIdentifiable get(Object key) {
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
  public Stream<ORID> getRids(final Object key) {
    final OTransactionIndexChanges indexChanges = database.getMicroOrRegularTransaction()
        .getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return super.getRids(key);
    }

    final Object collatedKey = getCollatingValue(key);

    ORID rid;
    if (!indexChanges.cleared) {
      // BEGIN FROM THE UNDERLYING RESULT SET
      //noinspection resource
      rid = super.getRids(key).findFirst().orElse(null);
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
  public Stream<ORawPair<Object, ORID>> stream() {
    final OTransactionIndexChanges indexChanges = database.getMicroOrRegularTransaction()
        .getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return super.stream();
    }

    final Stream<ORawPair<Object, ORID>> txStream = StreamSupport
        .stream(new PureTxBetweenIndexForwardSpliterator(null, true, null, true, indexChanges), false);
    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<Object, ORID>> backedStream = super.stream();
    return IndexStreamSecurityDecorator.decorateStream(this, mergeTxAndBackedStreams(indexChanges, txStream, backedStream, true));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> descStream() {
    final OTransactionIndexChanges indexChanges = database.getMicroOrRegularTransaction()
        .getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return super.descStream();
    }

    final Stream<ORawPair<Object, ORID>> txStream = StreamSupport
        .stream(new PureTxBetweenIndexBackwardSpliterator(null, true, null, true, indexChanges), false);
    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<Object, ORID>> backedStream = super.descStream();
    return IndexStreamSecurityDecorator.decorateStream(this, mergeTxAndBackedStreams(indexChanges, txStream, backedStream, false));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesBetween(Object fromKey, final boolean fromInclusive, Object toKey,
      final boolean toInclusive, final boolean ascOrder) {
    final OTransactionIndexChanges indexChanges = database.getMicroOrRegularTransaction()
        .getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return super.streamEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder);
    }

    fromKey = getCollatingValue(fromKey);
    toKey = getCollatingValue(toKey);

    final Stream<ORawPair<Object, ORID>> txStream;
    if (ascOrder) {
      //noinspection resource
      txStream = StreamSupport
          .stream(new PureTxBetweenIndexForwardSpliterator(fromKey, fromInclusive, toKey, toInclusive, indexChanges), false);
    } else {
      //noinspection resource
      txStream = StreamSupport
          .stream(new PureTxBetweenIndexBackwardSpliterator(fromKey, fromInclusive, toKey, toInclusive, indexChanges), false);
    }

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<Object, ORID>> backedStream = super
        .streamEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder);

    return IndexStreamSecurityDecorator
        .decorateStream(this, mergeTxAndBackedStreams(indexChanges, txStream, backedStream, ascOrder));
  }

  private Stream<ORawPair<Object, ORID>> mergeTxAndBackedStreams(OTransactionIndexChanges indexChanges,
      Stream<ORawPair<Object, ORID>> txStream, Stream<ORawPair<Object, ORID>> backedStream, boolean ascSortOrder) {
    return Streams.mergeSortedSpliterators(txStream,
        backedStream.map((entry) -> calculateTxIndexEntry(getCollatingValue(entry.first), entry.second, indexChanges))
            .filter(Objects::nonNull), (entryOne, entryTwo) -> ascSortOrder
            ? ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first)
            : -ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    final OTransactionIndexChanges indexChanges = database.getMicroOrRegularTransaction()
        .getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return super.streamEntriesMajor(fromKey, fromInclusive, ascOrder);
    }

    fromKey = getCollatingValue(fromKey);

    final Stream<ORawPair<Object, ORID>> txStream;

    final Object lastKey = indexChanges.getLastKey();
    if (ascOrder) {
      //noinspection resource
      txStream = StreamSupport
          .stream(new PureTxBetweenIndexForwardSpliterator(fromKey, fromInclusive, lastKey, true, indexChanges), false);
    } else {
      //noinspection resource
      txStream = StreamSupport
          .stream(new PureTxBetweenIndexBackwardSpliterator(fromKey, fromInclusive, lastKey, true, indexChanges), false);
    }

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<Object, ORID>> backedStream = super.streamEntriesMajor(fromKey, fromInclusive, ascOrder);
    return IndexStreamSecurityDecorator
        .decorateStream(this, mergeTxAndBackedStreams(indexChanges, txStream, backedStream, ascOrder));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    final OTransactionIndexChanges indexChanges = database.getMicroOrRegularTransaction()
        .getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return super.streamEntriesMinor(toKey, toInclusive, ascOrder);
    }

    toKey = getCollatingValue(toKey);

    final Stream<ORawPair<Object, ORID>> txStream;

    final Object firstKey = indexChanges.getFirstKey();
    if (ascOrder) {
      //noinspection resource
      txStream = StreamSupport
          .stream(new PureTxBetweenIndexForwardSpliterator(firstKey, true, toKey, toInclusive, indexChanges), false);
    } else {
      //noinspection resource
      txStream = StreamSupport
          .stream(new PureTxBetweenIndexBackwardSpliterator(firstKey, true, toKey, toInclusive, indexChanges), false);
    }

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<Object, ORID>> backedStream = super.streamEntriesMinor(toKey, toInclusive, ascOrder);
    return IndexStreamSecurityDecorator
        .decorateStream(this, mergeTxAndBackedStreams(indexChanges, txStream, backedStream, ascOrder));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> streamEntries(Collection<?> keys, boolean ascSortOrder) {
    final OTransactionIndexChanges indexChanges = database.getMicroOrRegularTransaction()
        .getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return super.streamEntries(keys, ascSortOrder);
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<Object, ORID>> txStream = keys.stream()
        .map((key) -> calculateTxIndexEntry(getCollatingValue(key), null, indexChanges)).filter(Objects::nonNull).sorted(
            (entryOne, entryTwo) -> ascSortOrder
                ? ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first)
                : -ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first));

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<Object, ORID>> backedStream = super.streamEntries(keys, ascSortOrder);
    return IndexStreamSecurityDecorator
        .decorateStream(this, mergeTxAndBackedStreams(indexChanges, txStream, backedStream, ascSortOrder));
  }

  private ORawPair<Object, ORID> calculateTxIndexEntry(Object key, final ORID backendValue,
      final OTransactionIndexChanges indexChanges) {
    key = getCollatingValue(key);
    ORID result = backendValue;
    final OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerKey(key);
    if (changesPerKey.entries.isEmpty()) {
      if (backendValue == null) {
        return null;
      } else {
        return new ORawPair<>(key, backendValue);
      }
    }

    for (OTransactionIndexEntry entry : changesPerKey.entries) {
      if (entry.operation == OPERATION.REMOVE)
        result = null;
      else if (entry.operation == OPERATION.PUT)
        result = entry.value.getIdentity();
    }

    if (result == null) {
      return null;
    }

    return new ORawPair<>(key, result);
  }
}

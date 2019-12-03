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
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;

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
public class OIndexTxAwareMultiValue extends OIndexTxAware<Collection<OIdentifiable>> {
  private class PureTxBetweenIndexForwardSpliterator implements Spliterator<ORawPair<Object, ORID>> {
    private final OTransactionIndexChanges indexChanges;
    private       Object                   lastKey;

    private Object nextKey;

    private Iterator<OIdentifiable> valuesIterator = new OEmptyIterator<>();
    private Object                  key;

    private PureTxBetweenIndexForwardSpliterator(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive,
        OTransactionIndexChanges indexChanges) {
      this.indexChanges = indexChanges;

      fromKey = enhanceFromCompositeKeyBetweenAsc(fromKey, fromInclusive);
      toKey = enhanceToCompositeKeyBetweenAsc(toKey, toInclusive);

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
      if (valuesIterator.hasNext()) {
        final ORawPair<Object, ORID> entry = nextEntryInternal();
        action.accept(entry);
        return true;
      }

      if (nextKey == null) {
        return false;
      }

      Set<OIdentifiable> result;
      do {
        result = calculateTxValue(nextKey, indexChanges);
        key = nextKey;

        nextKey = indexChanges.getHigherKey(nextKey);

        if (nextKey != null && ODefaultComparator.INSTANCE.compare(nextKey, lastKey) > 0)
          nextKey = null;
      } while ((result == null || result.isEmpty()) && nextKey != null);

      if (result == null || result.isEmpty()) {
        return false;
      }

      valuesIterator = result.iterator();
      final ORawPair<Object, ORID> entry = nextEntryInternal();
      action.accept(entry);

      return true;
    }

    private ORawPair<Object, ORID> nextEntryInternal() {
      final OIdentifiable identifiable = valuesIterator.next();
      return new ORawPair<>(key, identifiable.getIdentity());
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
      return NONNULL | ORDERED | SORTED;
    }

    @Override
    public Comparator<? super ORawPair<Object, ORID>> getComparator() {
      return (entryOne, entryTwo) -> ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
    }
  }

  private class PureTxBetweenIndexBackwardCursor implements Spliterator<ORawPair<Object, ORID>> {
    private final OTransactionIndexChanges indexChanges;
    private       Object                   firstKey;

    private Object nextKey;

    private Iterator<OIdentifiable> valuesIterator = new OEmptyIterator<>();
    private Object                  key;

    private PureTxBetweenIndexBackwardCursor(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive,
        OTransactionIndexChanges indexChanges) {
      this.indexChanges = indexChanges;

      fromKey = enhanceFromCompositeKeyBetweenDesc(fromKey, fromInclusive);
      toKey = enhanceToCompositeKeyBetweenDesc(toKey, toInclusive);

      final Object[] keys = indexChanges.firstAndLastKeys(fromKey, fromInclusive, toKey, toInclusive);
      if (keys.length == 0) {
        nextKey = null;
      } else {
        firstKey = keys[0];
        nextKey = keys[1];
      }
    }

    private ORawPair<Object, ORID> nextEntryInternal() {
      final OIdentifiable identifiable = valuesIterator.next();
      return new ORawPair<>(key, identifiable.getIdentity());
    }

    @Override
    public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
      if (valuesIterator.hasNext()) {
        final ORawPair<Object, ORID> entry = nextEntryInternal();
        action.accept(entry);
        return true;
      }

      if (nextKey == null) {
        return false;
      }

      Set<OIdentifiable> result;
      do {
        result = calculateTxValue(nextKey, indexChanges);
        key = nextKey;

        nextKey = indexChanges.getLowerKey(nextKey);

        if (nextKey != null && ODefaultComparator.INSTANCE.compare(nextKey, firstKey) < 0)
          nextKey = null;
      } while ((result == null || result.isEmpty()) && nextKey != null);

      if (result == null || result.isEmpty()) {
        return false;
      }

      valuesIterator = result.iterator();
      final ORawPair<Object, ORID> entry = nextEntryInternal();
      action.accept(entry);
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
      return NONNULL | ORDERED | SORTED;
    }

    @Override
    public Comparator<? super ORawPair<Object, ORID>> getComparator() {
      return (entryOne, entryTwo) -> -ODefaultComparator.INSTANCE.compare(entryOne.first, entryTwo.first);
    }
  }

  public OIndexTxAwareMultiValue(final ODatabaseDocumentInternal database, final OIndex delegate) {
    super(database, delegate);
  }

  @Override
  public Collection<OIdentifiable> get(Object key) {
    final OTransactionIndexChanges indexChanges = database.getMicroOrRegularTransaction()
        .getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      @SuppressWarnings("unchecked")
      Collection<OIdentifiable> res = (Collection<OIdentifiable>) super.get(key);
      //In case of active transaction we use to return null instead of empty list, make check to be backward compatible
      if (database.getTransaction().isActive()
          && ((OTransactionOptimistic) database.getTransaction()).getIndexOperations().size() != 0 && res.isEmpty())
        return null;
      //noinspection unchecked
      return OIndexInternal.securityFilterOnRead(this, res);
    }

    key = getCollatingValue(key);

    final Set<OIdentifiable> result = new HashSet<>();
    if (!indexChanges.cleared) {
      // BEGIN FROM THE UNDERLYING RESULT SET
      @SuppressWarnings("unchecked")
      final Collection<OIdentifiable> subResult = (Collection<OIdentifiable>) super.get(key);
      if (subResult != null) {
        result.addAll(subResult);
      }
    }

    final Set<OIdentifiable> processed = new HashSet<>();
    for (OIdentifiable identifiable : result) {
      ORawPair<Object, ORID> entry = calculateTxIndexEntry(key, identifiable.getIdentity(), indexChanges);
      if (entry != null) {
        processed.add(entry.second);
      }
    }

    Set<OIdentifiable> txChanges = calculateTxValue(key, indexChanges);
    if (txChanges != null)
      processed.addAll(txChanges);

    if (!processed.isEmpty()) {
      //noinspection unchecked
      return OIndexInternal.securityFilterOnRead(this, processed);
    }

    return null;
  }

  @Override
  public boolean contains(final Object key) {
    final Collection<OIdentifiable> result = get(key);
    return result != null && !result.isEmpty();
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesBetween(Object fromKey, final boolean fromInclusive, Object toKey,
      final boolean toInclusive, final boolean ascOrder) {

    final OTransactionIndexChanges indexChanges = database.getMicroOrRegularTransaction()
        .getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return super.iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder);
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
          .stream(new PureTxBetweenIndexBackwardCursor(fromKey, fromInclusive, toKey, toInclusive, indexChanges), false);
    }

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<Object, ORID>> backedStream = super
        .iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder);

    return IndexStreamSecurityDecorator
        .decorateStream(this, mergeTxAndBackedStreams(indexChanges, txStream, backedStream, ascOrder));
  }

  private Stream<ORawPair<Object, ORID>> mergeTxAndBackedStreams(OTransactionIndexChanges indexChanges,
      Stream<ORawPair<Object, ORID>> txStream, Stream<ORawPair<Object, ORID>> backedStream, boolean ascOrder) {
    return Streams.mergeSortedSpliterators(txStream,
        backedStream.map((entry) -> calculateTxIndexEntry(entry.first, entry.second, indexChanges)).filter(Objects::nonNull),
        (entryOne, entryTwo) -> ascOrder
            ? ODefaultComparator.INSTANCE.compare(getCollatingValue(entryOne.first), getCollatingValue(entryTwo.first))
            : -ODefaultComparator.INSTANCE.compare(getCollatingValue(entryOne.first), getCollatingValue(entryTwo.first)));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    final OTransactionIndexChanges indexChanges = database.getMicroOrRegularTransaction()
        .getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return super.iterateEntriesMajor(fromKey, fromInclusive, ascOrder);
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
          .stream(new PureTxBetweenIndexBackwardCursor(fromKey, fromInclusive, lastKey, true, indexChanges), false);
    }

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<Object, ORID>> backedStream = super.iterateEntriesMajor(fromKey, fromInclusive, ascOrder);

    return IndexStreamSecurityDecorator
        .decorateStream(this, mergeTxAndBackedStreams(indexChanges, txStream, backedStream, ascOrder));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    final OTransactionIndexChanges indexChanges = database.getMicroOrRegularTransaction()
        .getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return super.iterateEntriesMinor(toKey, toInclusive, ascOrder);
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
          .stream(new PureTxBetweenIndexBackwardCursor(firstKey, true, toKey, toInclusive, indexChanges), false);
    }

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<Object, ORID>> backedCursor = super.iterateEntriesMinor(toKey, toInclusive, ascOrder);
    return IndexStreamSecurityDecorator
        .decorateStream(this, mergeTxAndBackedStreams(indexChanges, txStream, backedCursor, ascOrder));
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntries(Collection<?> keys, boolean ascSortOrder) {
    final OTransactionIndexChanges indexChanges = database.getMicroOrRegularTransaction()
        .getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return super.iterateEntries(keys, ascSortOrder);
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<Object, ORID>> txStream = keys.stream().flatMap((key) -> {
      final Set<OIdentifiable> result = calculateTxValue(getCollatingValue(key), indexChanges);
      if (result != null) {
        return result.stream().map((rid) -> new ORawPair<>(getCollatingValue(key), rid.getIdentity()));
      }
      return null;
    }).filter(Objects::nonNull).sorted((entryOne, entryTwo) -> ascSortOrder
        ? ODefaultComparator.INSTANCE.compare(getCollatingValue(entryOne.first), getCollatingValue(entryTwo.first))
        : -ODefaultComparator.INSTANCE.compare(getCollatingValue(entryOne.first), getCollatingValue(entryTwo.first)));

    if (indexChanges.cleared) {
      return IndexStreamSecurityDecorator.decorateStream(this, txStream);
    }

    @SuppressWarnings("resource")
    final Stream<ORawPair<Object, ORID>> backedCursor = super.iterateEntries(keys, ascSortOrder);
    return IndexStreamSecurityDecorator
        .decorateStream(this, mergeTxAndBackedStreams(indexChanges, txStream, backedCursor, ascSortOrder));
  }

  private ORawPair<Object, ORID> calculateTxIndexEntry(Object key, final ORID backendValue, OTransactionIndexChanges indexChanges) {
    key = getCollatingValue(key);
    final OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerKey(key);
    if (changesPerKey.entries.isEmpty()) {
      return new ORawPair<>(key, backendValue);
    }

    int putCounter = 1;
    for (OTransactionIndexEntry entry : changesPerKey.entries) {
      if (entry.operation == OPERATION.PUT && entry.value.equals(backendValue))
        putCounter++;
      else if (entry.operation == OPERATION.REMOVE) {
        if (entry.value == null)
          putCounter = 0;
        else if (entry.value.equals(backendValue) && putCounter > 0)
          putCounter--;
      }
    }

    if (putCounter <= 0) {
      return null;
    }

    return new ORawPair<>(key, backendValue);
  }

  private static Set<OIdentifiable> calculateTxValue(final Object key, OTransactionIndexChanges indexChanges) {
    final List<OIdentifiable> result = new ArrayList<>();
    final OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerKey(key);
    if (changesPerKey.entries.isEmpty()) {
      return null;
    }

    for (OTransactionIndexEntry entry : changesPerKey.entries) {
      if (entry.operation == OPERATION.REMOVE) {
        if (entry.value == null)
          result.clear();
        else
          result.remove(entry.value);
      } else
        result.add(entry.value);
    }
    if (result.isEmpty())
      return null;

    return new HashSet<>(result);
  }
}

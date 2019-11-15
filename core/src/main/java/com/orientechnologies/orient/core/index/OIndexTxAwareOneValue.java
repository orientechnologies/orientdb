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

/**
 * Transactional wrapper for indexes. Stores changes locally to the transaction until tx.commit(). All the other operations are
 * delegated to the wrapped OIndex instance.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OIndexTxAwareOneValue extends OIndexTxAware<OIdentifiable> {
  private class PureTxBetweenIndexForwardCursor implements IndexCursor {
    private final OTransactionIndexChanges indexChanges;
    private       Object                   lastKey;

    private Object nextKey;

    private PureTxBetweenIndexForwardCursor(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive,
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
      return NONNULL | ORDERED;
    }
  }

  private class PureTxBetweenIndexBackwardCursor implements IndexCursor {
    private final OTransactionIndexChanges indexChanges;
    private       Object                   firstKey;

    private Object nextKey;

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
      return NONNULL | ORDERED;
    }
  }

  private static class IndexTxCursor implements IndexCursor {

    private final IndexCursor              backedCursor;
    private final boolean                  ascOrder;
    private final OTransactionIndexChanges indexChanges;
    private final IndexCursor              txBetweenIndexCursor;

    private ORawPair<Object, ORID> nextTxEntry;
    private ORawPair<Object, ORID> nextBackedEntry;

    private boolean firstTime;

    private IndexTxCursor(IndexCursor txCursor, IndexCursor backedCursor, boolean ascOrder, OTransactionIndexChanges indexChanges) {
      this.backedCursor = backedCursor;
      this.ascOrder = ascOrder;
      this.indexChanges = indexChanges;
      txBetweenIndexCursor = txCursor;
      firstTime = true;
    }

    private ORawPair<Object, ORID> nextTxEntry() {
      @SuppressWarnings("unchecked")
      final ORawPair<Object, ORID>[] entry = new ORawPair[1];
      final ORawPair<Object, ORID> result = nextTxEntry;

      if (txBetweenIndexCursor.tryAdvance((pair) -> entry[0] = pair)) {
        nextTxEntry = entry[0];
      } else {
        nextTxEntry = null;
      }

      return result;
    }

    private ORawPair<Object, ORID> nextBackedEntry() {
      final ORawPair<Object, ORID> result = calculateTxIndexEntry(nextBackedEntry.first, nextBackedEntry.second, indexChanges);

      @SuppressWarnings("unchecked")
      final ORawPair<Object, ORID>[] entry = new ORawPair[1];
      if (backedCursor.tryAdvance((pair) -> entry[0] = pair)) {
        nextBackedEntry = entry[0];
      } else {
        nextBackedEntry = null;
      }

      return result;
    }

    @Override
    public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
      if (firstTime) {
        @SuppressWarnings("unchecked")
        final ORawPair<Object, ORID>[] entry = new ORawPair[1];
        if (txBetweenIndexCursor.tryAdvance((pair) -> entry[0] = pair)) {
          nextTxEntry = entry[0];
        } else {
          nextTxEntry = null;
        }

        if (backedCursor.tryAdvance((pair) -> entry[0] = pair)) {
          nextBackedEntry = entry[0];
        } else {
          nextBackedEntry = null;
        }

        firstTime = false;
      }

      ORawPair<Object, ORID> result = null;

      while (result == null && (nextTxEntry != null || nextBackedEntry != null)) {
        if (nextTxEntry == null) {
          result = nextBackedEntry();
        } else if (nextBackedEntry == null) {
          result = nextTxEntry();
        } else {
          if (ascOrder) {
            if (ODefaultComparator.INSTANCE.compare(nextBackedEntry.first, nextTxEntry.first) <= 0) {
              result = nextBackedEntry();
            } else {
              result = nextTxEntry();
            }
          } else {
            if (ODefaultComparator.INSTANCE.compare(nextBackedEntry.first, nextTxEntry.first) >= 0) {
              result = nextBackedEntry();
            } else {
              result = nextTxEntry();
            }
          }
        }
      }

      if (result == null) {
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
      return NONNULL | ORDERED;
    }
  }

  public OIndexTxAwareOneValue(final ODatabaseDocumentInternal iDatabase, final OIndex<OIdentifiable> iDelegate) {
    super(iDatabase, iDelegate);
  }

  @Override
  public OIdentifiable get(Object key) {
    final OTransactionIndexChanges indexChanges = database.getMicroOrRegularTransaction()
        .getIndexChangesInternal(delegate.getName());
    if (indexChanges == null)
      return super.get(key);

    key = getCollatingValue(key);

    ORID result;
    if (!indexChanges.cleared) {
      // BEGIN FROM THE UNDERLYING RESULT SET
      result = super.get(key).getIdentity();
    } else {
      // BEGIN FROM EMPTY RESULT SET
      result = null;
    }

    // FILTER RESULT SET WITH TRANSACTIONAL CHANGES
    final ORawPair<Object, ORID> entry = calculateTxIndexEntry(key, result, indexChanges);
    if (entry == null) {
      return null;
    }

    return OIndexInternal.securityFilterOnRead(this, entry.second);
  }

  @Override
  public boolean contains(final Object key) {
    return get(key) != null;
  }

  @Override
  public IndexCursor iterateEntriesBetween(Object fromKey, final boolean fromInclusive, Object toKey, final boolean toInclusive,
      final boolean ascOrder) {
    final OTransactionIndexChanges indexChanges = database.getMicroOrRegularTransaction()
        .getIndexChangesInternal(delegate.getName());
    if (indexChanges == null)
      return super.iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder);

    fromKey = getCollatingValue(fromKey);
    toKey = getCollatingValue(toKey);

    final IndexCursor txCursor;
    if (ascOrder)
      txCursor = new PureTxBetweenIndexForwardCursor(fromKey, fromInclusive, toKey, toInclusive, indexChanges);
    else
      txCursor = new PureTxBetweenIndexBackwardCursor(fromKey, fromInclusive, toKey, toInclusive, indexChanges);

    if (indexChanges.cleared)
      return new IndexCursorSecurityDecorator(txCursor, this);

    final IndexCursor backedCursor = super.iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder);

    return new IndexCursorSecurityDecorator(new IndexTxCursor(txCursor, backedCursor, ascOrder, indexChanges), this);
  }

  @Override
  public IndexCursor iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    final OTransactionIndexChanges indexChanges = database.getMicroOrRegularTransaction()
        .getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return super.iterateEntriesMajor(fromKey, fromInclusive, ascOrder);
    }

    fromKey = getCollatingValue(fromKey);

    final IndexCursor txCursor;

    final Object lastKey = indexChanges.getLastKey();
    if (ascOrder)
      txCursor = new PureTxBetweenIndexForwardCursor(fromKey, fromInclusive, lastKey, true, indexChanges);
    else
      txCursor = new PureTxBetweenIndexBackwardCursor(fromKey, fromInclusive, lastKey, true, indexChanges);

    if (indexChanges.cleared) {
      return new IndexCursorSecurityDecorator(txCursor, this);
    }

    final IndexCursor backedCursor = super.iterateEntriesMajor(fromKey, fromInclusive, ascOrder);

    return new IndexCursorSecurityDecorator(new IndexTxCursor(txCursor, backedCursor, ascOrder, indexChanges), this);
  }

  @Override
  public IndexCursor iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    final OTransactionIndexChanges indexChanges = database.getMicroOrRegularTransaction()
        .getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return super.iterateEntriesMinor(toKey, toInclusive, ascOrder);
    }

    toKey = getCollatingValue(toKey);

    final IndexCursor txCursor;

    final Object firstKey = indexChanges.getFirstKey();
    if (ascOrder) {
      txCursor = new PureTxBetweenIndexForwardCursor(firstKey, true, toKey, toInclusive, indexChanges);
    } else {
      txCursor = new PureTxBetweenIndexBackwardCursor(firstKey, true, toKey, toInclusive, indexChanges);
    }

    if (indexChanges.cleared) {
      return new IndexCursorSecurityDecorator(txCursor, this);
    }

    final IndexCursor backedCursor = super.iterateEntriesMinor(toKey, toInclusive, ascOrder);
    return new IndexCursorSecurityDecorator(new IndexTxCursor(txCursor, backedCursor, ascOrder, indexChanges), this);
  }

  @Override
  public IndexCursor iterateEntries(Collection<?> keys, boolean ascSortOrder) {
    final OTransactionIndexChanges indexChanges = database.getMicroOrRegularTransaction()
        .getIndexChangesInternal(delegate.getName());
    if (indexChanges == null) {
      return super.iterateEntries(keys, ascSortOrder);
    }

    final List<Object> sortedKeys = new ArrayList<>(keys.size());
    for (Object key : keys)
      sortedKeys.add(getCollatingValue(key));

    if (ascSortOrder) {
      sortedKeys.sort(ODefaultComparator.INSTANCE);
    } else {
      sortedKeys.sort(Collections.reverseOrder(ODefaultComparator.INSTANCE));
    }

    final IndexCursor txCursor = new IndexCursor() {
      private Iterator<Object> keysIterator = sortedKeys.iterator();

      @Override
      public boolean tryAdvance(Consumer<? super ORawPair<Object, ORID>> action) {
        if (keysIterator == null) {
          return false;
        }

        ORawPair<Object, ORID> entry = null;
        while (entry == null && keysIterator.hasNext()) {
          final Object key = keysIterator.next();

          entry = calculateTxIndexEntry(key, null, indexChanges);
        }

        if (entry == null) {
          keysIterator = null;
          return false;
        }

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
        return NONNULL | ORDERED;
      }
    };

    if (indexChanges.cleared) {
      return new IndexCursorSecurityDecorator(txCursor, this);
    }

    final IndexCursor backedCursor = super.iterateEntries(keys, ascSortOrder);
    return new IndexCursorSecurityDecorator(new IndexTxCursor(txCursor, backedCursor, ascSortOrder, indexChanges), this);
  }

  private static ORawPair<Object, ORID> calculateTxIndexEntry(final Object key, final ORID backendValue,
      final OTransactionIndexChanges indexChanges) {
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

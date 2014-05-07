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
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.engine.local.OEngineLocal;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;

/**
 * Transactional wrapper for indexes. Stores changes locally to the transaction until tx.commit(). All the other operations are
 * delegated to the wrapped OIndex instance.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexTxAwareOneValue extends OIndexTxAware<OIdentifiable> {
  public OIndexTxAwareOneValue(final ODatabaseRecord iDatabase, final OIndex<OIdentifiable> iDelegate) {
    super(iDatabase, iDelegate);
  }

  @Override
  public void checkEntry(final OIdentifiable iRecord, final Object iKey) {
    // CHECK IF ALREADY EXISTS IN TX
    String storageType = database.getStorage().getType();
    if (storageType.equals(OEngineMemory.NAME) || storageType.equals(OEngineLocal.NAME) || !database.getTransaction().isActive()) {
      final OIdentifiable previousRecord = get(iKey);
      if (previousRecord != null && !previousRecord.equals(iRecord))
        throw new ORecordDuplicatedException(String.format(
            "Cannot index record %s: found duplicated key '%s' in index '%s' previously assigned to the record %s", iRecord, iKey,
            getName(), previousRecord), previousRecord.getIdentity());

      super.checkEntry(iRecord, iKey);
    }
  }

  @Override
  public OIdentifiable get(final Object key) {
    final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());

    if (indexChanges == null)
      return super.get(key);

    OIdentifiable result;
    if (!indexChanges.cleared)
      // BEGIN FROM THE UNDERLYING RESULT SET
      result = super.get(key);
    else
      // BEGIN FROM EMPTY RESULT SET
      result = null;

    // FILTER RESULT SET WITH TRANSACTIONAL CHANGES
    final Map.Entry<Object, OIdentifiable> entry = calculateTxIndexEntry(key, result, indexChanges);
    if (entry == null)
      return null;

    return entry.getValue();
  }

  @Override
  public boolean contains(final Object key) {
    return get(key) != null;
  }

  @Override
  public OIndexCursor iterateEntriesBetween(final Object fromKey, final boolean fromInclusive, final Object toKey,
      final boolean toInclusive, final boolean ascOrder) {

    final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());
    if (indexChanges == null)
      return super.iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder);

    final OIndexCursor txCursor;
    if (ascOrder)
      txCursor = new PureTxBetweenIndexForwardCursor(fromKey, fromInclusive, toKey, toInclusive, indexChanges);
    else
      txCursor = new PureTxBetweenIndexBackwardCursor(fromKey, fromInclusive, toKey, toInclusive, indexChanges);

    if (indexChanges.cleared)
      return txCursor;

    final OIndexCursor backedCursor = super.iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder);

    return new OIndexTxCursor(txCursor, backedCursor, ascOrder, indexChanges);
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean fromInclusive, boolean ascOrder) {
    final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());
    if (indexChanges == null)
      return super.iterateEntriesMajor(fromKey, fromInclusive, ascOrder);

    final OIndexCursor txCursor;

    final Object lastKey = indexChanges.getLastKey();
    if (ascOrder)
      txCursor = new PureTxBetweenIndexForwardCursor(fromKey, fromInclusive, lastKey, true, indexChanges);
    else
      txCursor = new PureTxBetweenIndexBackwardCursor(fromKey, fromInclusive, lastKey, true, indexChanges);

    if (indexChanges.cleared)
      return txCursor;

    final OIndexCursor backedCursor = super.iterateEntriesMajor(fromKey, fromInclusive, ascOrder);

    return new OIndexTxCursor(txCursor, backedCursor, ascOrder, indexChanges);
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean toInclusive, boolean ascOrder) {
    final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());
    if (indexChanges == null)
      return super.iterateEntriesMinor(toKey, toInclusive, ascOrder);

    final OIndexCursor txCursor;

    final Object firstKey = indexChanges.getFirstKey();
    if (ascOrder)
      txCursor = new PureTxBetweenIndexForwardCursor(firstKey, true, toKey, toInclusive, indexChanges);
    else
      txCursor = new PureTxBetweenIndexBackwardCursor(firstKey, true, toKey, toInclusive, indexChanges);

    if (indexChanges.cleared)
      return txCursor;

    final OIndexCursor backedCursor = super.iterateEntriesMinor(toKey, toInclusive, ascOrder);
    return new OIndexTxCursor(txCursor, backedCursor, ascOrder, indexChanges);
  }

  @Override
  public OIndexCursor iterateEntries(Collection<?> keys, boolean ascSortOrder) {
    final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());
    if (indexChanges == null)
      return super.iterateEntries(keys, ascSortOrder);

    final List<Object> sortedKeys = new ArrayList<Object>(keys);
    if (ascSortOrder)
      Collections.sort(sortedKeys, ODefaultComparator.INSTANCE);
    else
      Collections.sort(sortedKeys, Collections.reverseOrder(ODefaultComparator.INSTANCE));

    final OIndexCursor txCursor = new OIndexAbstractCursor() {
      private Iterator<Object> keysIterator = sortedKeys.iterator();

      @Override
      public Map.Entry<Object, OIdentifiable> nextEntry() {
        if (keysIterator == null)
          return null;

        Map.Entry<Object, OIdentifiable> entry = null;
        while (entry == null && keysIterator.hasNext()) {
          final Object key = keysIterator.next();

          entry = calculateTxIndexEntry(key, null, indexChanges);
        }

        if (entry == null) {
          keysIterator = null;
          return null;
        }

        return entry;
      }
    };

    if (indexChanges.cleared)
      return txCursor;

    final OIndexCursor backedCursor = super.iterateEntries(keys, ascSortOrder);
    return new OIndexTxCursor(txCursor, backedCursor, ascSortOrder, indexChanges);
  }

  private Map.Entry<Object, OIdentifiable> calculateTxIndexEntry(final Object key, final OIdentifiable backendValue,
      final OTransactionIndexChanges indexChanges) {
    final OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerKey(key);
    if (changesPerKey.entries.isEmpty()) {
      if (backendValue == null)
        return null;
      else
        return createMapEntry(key, backendValue);
    }

    OIdentifiable result = backendValue;

    for (OTransactionIndexEntry entry : changesPerKey.entries) {
      if (entry.operation == OPERATION.REMOVE)
        result = null;
      else if (entry.operation == OPERATION.PUT)
        result = entry.value;
    }

    if (result == null)
      return null;

    final OIdentifiable resultValue = result;
    return createMapEntry(key, resultValue);
  }

  private Map.Entry<Object, OIdentifiable> createMapEntry(final Object key, final OIdentifiable resultValue) {
    return new Map.Entry<Object, OIdentifiable>() {
      @Override
      public Object getKey() {
        return key;
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

  private class PureTxBetweenIndexForwardCursor extends OIndexAbstractCursor {
    private final OTransactionIndexChanges indexChanges;
    private Object                         firstKey;
    private Object                         lastKey;

    private Object                         nextKey;

    public PureTxBetweenIndexForwardCursor(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive,
        OTransactionIndexChanges indexChanges) {
      this.indexChanges = indexChanges;

      fromKey = enhanceFromCompositeKeyBetweenAsc(fromKey, fromInclusive);
      toKey = enhanceToCompositeKeyBetweenAsc(toKey, toInclusive);

      if (toInclusive)
        firstKey = indexChanges.getCeilingKey(fromKey);
      else
        firstKey = indexChanges.getHigherKey(fromKey);

      if (fromInclusive)
        lastKey = indexChanges.getFloorKey(toKey);
      else
        lastKey = indexChanges.getLowerKey(toKey);

      nextKey = firstKey;
    }

    @Override
    public Map.Entry<Object, OIdentifiable> nextEntry() {
      if (nextKey == null)
        return null;

      Map.Entry<Object, OIdentifiable> result;

      do {
        result = calculateTxIndexEntry(nextKey, null, indexChanges);
        nextKey = indexChanges.getHigherKey(nextKey);

        if (nextKey != null && ODefaultComparator.INSTANCE.compare(nextKey, lastKey) > 0)
          nextKey = null;

      } while (result == null && nextKey != null);

      return result;
    }
  }

  private class PureTxBetweenIndexBackwardCursor extends OIndexAbstractCursor {
    private final OTransactionIndexChanges indexChanges;
    private Object                         firstKey;
    private Object                         lastKey;

    private Object                         nextKey;

    public PureTxBetweenIndexBackwardCursor(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive,
        OTransactionIndexChanges indexChanges) {
      this.indexChanges = indexChanges;

      fromKey = enhanceFromCompositeKeyBetweenDesc(fromKey, fromInclusive);
      toKey = enhanceToCompositeKeyBetweenDesc(toKey, toInclusive);

      if (toInclusive)
        firstKey = indexChanges.getCeilingKey(fromKey);
      else
        firstKey = indexChanges.getHigherKey(fromKey);

      if (fromInclusive)
        lastKey = indexChanges.getFloorKey(toKey);
      else
        lastKey = indexChanges.getLowerKey(toKey);

      nextKey = lastKey;
    }

    @Override
    public Map.Entry<Object, OIdentifiable> nextEntry() {
      if (nextKey == null)
        return null;

      Map.Entry<Object, OIdentifiable> result;
      do {
        result = calculateTxIndexEntry(nextKey, null, indexChanges);
        nextKey = indexChanges.getLowerKey(nextKey);

        if (nextKey != null && ODefaultComparator.INSTANCE.compare(nextKey, firstKey) < 0)
          nextKey = null;
      } while (result == null && nextKey != null);

      return result;
    }
  }

  private class OIndexTxCursor extends OIndexAbstractCursor {

    private final OIndexCursor               backedCursor;
    private final boolean                    ascOrder;
    private final OTransactionIndexChanges   indexChanges;
    private OIndexCursor                     txBetweenIndexCursor;

    private Map.Entry<Object, OIdentifiable> nextTxEntry;
    private Map.Entry<Object, OIdentifiable> nextBackedEntry;

    private boolean                          firstTime;

    public OIndexTxCursor(OIndexCursor txCursor, OIndexCursor backedCursor, boolean ascOrder, OTransactionIndexChanges indexChanges) {
      this.backedCursor = backedCursor;
      this.ascOrder = ascOrder;
      this.indexChanges = indexChanges;
      txBetweenIndexCursor = txCursor;
      firstTime = true;
    }

    @Override
    public Map.Entry<Object, OIdentifiable> nextEntry() {
      if (firstTime) {
        nextTxEntry = txBetweenIndexCursor.nextEntry();
        nextBackedEntry = backedCursor.nextEntry();
        firstTime = false;
      }

      Map.Entry<Object, OIdentifiable> result = null;

      while (result == null && (nextTxEntry != null || nextBackedEntry != null)) {
        if (nextTxEntry == null && nextBackedEntry != null) {
          result = nextBackedEntry(getPrefetchSize());
        } else if (nextBackedEntry == null && nextTxEntry != null) {
          result = nextTxEntry(getPrefetchSize());
        } else if (nextTxEntry != null && nextBackedEntry != null) {
          if (ascOrder) {
            if (ODefaultComparator.INSTANCE.compare(nextBackedEntry.getKey(), nextTxEntry.getKey()) <= 0) {
              result = nextBackedEntry(getPrefetchSize());
            } else {
              result = nextTxEntry(getPrefetchSize());
            }
          } else {
            if (ODefaultComparator.INSTANCE.compare(nextBackedEntry.getKey(), nextTxEntry.getKey()) >= 0) {
              result = nextBackedEntry(getPrefetchSize());
            } else {
              result = nextTxEntry(getPrefetchSize());
            }
          }
        }
      }

      return result;
    }

    private Map.Entry<Object, OIdentifiable> nextTxEntry(int prefetchSize) {
      Map.Entry<Object, OIdentifiable> result = nextTxEntry;
      nextTxEntry = txBetweenIndexCursor.nextEntry();
      return result;
    }

    private Map.Entry<Object, OIdentifiable> nextBackedEntry(int prefetchSize) {
      Map.Entry<Object, OIdentifiable> result;
      result = calculateTxIndexEntry(nextBackedEntry.getKey(), nextBackedEntry.getValue(), indexChanges);
      nextBackedEntry = backedCursor.nextEntry();
      return result;
    }
  }
}

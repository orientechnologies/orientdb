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
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;

import java.util.*;

/**
 * Transactional wrapper for indexes. Stores changes locally to the transaction until tx.commit(). All the other operations are
 * delegated to the wrapped OIndex instance.
 *
 * @author Luca Garulli
 */
public class OIndexTxAwareMultiValue extends OIndexTxAware<Set<OIdentifiable>> {
  private static class MapEntry implements Map.Entry<Object, OIdentifiable> {
    private final Object        key;
    private final OIdentifiable backendValue;

    public MapEntry(Object key, OIdentifiable backendValue) {
      this.key = key;
      this.backendValue = backendValue;
    }

    @Override
    public Object getKey() {
      return key;
    }

    @Override
    public OIdentifiable getValue() {
      return backendValue;
    }

    @Override
    public OIdentifiable setValue(OIdentifiable value) {
      throw new UnsupportedOperationException("setValue");
    }
  }

  private class PureTxBetweenIndexForwardCursor extends OIndexAbstractCursor {
    private final OTransactionIndexChanges indexChanges;
    private       Object                   firstKey;
    private       Object                   lastKey;

    private Object nextKey;

    private Iterator<OIdentifiable> valuesIterator = new OEmptyIterator<OIdentifiable>();
    private Object key;

    public PureTxBetweenIndexForwardCursor(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive,
        OTransactionIndexChanges indexChanges) {
      this.indexChanges = indexChanges;

      fromKey = enhanceFromCompositeKeyBetweenAsc(fromKey, fromInclusive);
      toKey = enhanceToCompositeKeyBetweenAsc(toKey, toInclusive);

      if (fromInclusive)
        firstKey = indexChanges.getCeilingKey(fromKey);
      else
        firstKey = indexChanges.getHigherKey(fromKey);

      if (toInclusive)
        lastKey = indexChanges.getFloorKey(toKey);
      else
        lastKey = indexChanges.getLowerKey(toKey);

      nextKey = firstKey;
    }

    @Override
    public Map.Entry<Object, OIdentifiable> nextEntry() {
      if (valuesIterator.hasNext())
        return nextEntryInternal();

      if (nextKey == null)
        return null;

      Set<OIdentifiable> result;
      do {
        result = calculateTxValue(nextKey, indexChanges);
        key = nextKey;

        nextKey = indexChanges.getHigherKey(nextKey);

        if (nextKey != null && ODefaultComparator.INSTANCE.compare(nextKey, lastKey) > 0)
          nextKey = null;
      } while ((result == null || result.isEmpty()) && nextKey != null);

      if (result == null || result.isEmpty())
        return null;

      valuesIterator = result.iterator();
      return nextEntryInternal();
    }

    Map.Entry<Object, OIdentifiable> nextEntryInternal() {
      final OIdentifiable identifiable = valuesIterator.next();
      return new Map.Entry<Object, OIdentifiable>() {
        @Override
        public Object getKey() {
          return key;
        }

        @Override
        public OIdentifiable getValue() {
          return identifiable;
        }

        @Override
        public OIdentifiable setValue(OIdentifiable value) {
          throw new UnsupportedOperationException("setValue");
        }
      };
    }
  }

  private class PureTxBetweenIndexBackwardCursor extends OIndexAbstractCursor {
    private final OTransactionIndexChanges indexChanges;
    private       Object                   firstKey;
    private       Object                   lastKey;

    private Object nextKey;

    private Iterator<OIdentifiable> valuesIterator = new OEmptyIterator<OIdentifiable>();
    private Object key;

    public PureTxBetweenIndexBackwardCursor(Object fromKey, boolean fromInclusive, Object toKey, boolean toInclusive,
        OTransactionIndexChanges indexChanges) {
      this.indexChanges = indexChanges;

      fromKey = enhanceFromCompositeKeyBetweenDesc(fromKey, fromInclusive);
      toKey = enhanceToCompositeKeyBetweenDesc(toKey, toInclusive);

      if (fromInclusive)
        firstKey = indexChanges.getCeilingKey(fromKey);
      else
        firstKey = indexChanges.getHigherKey(fromKey);

      if (toInclusive)
        lastKey = indexChanges.getFloorKey(toKey);
      else
        lastKey = indexChanges.getLowerKey(toKey);

      nextKey = lastKey;
    }

    @Override
    public Map.Entry<Object, OIdentifiable> nextEntry() {
      if (valuesIterator.hasNext())
        return nextEntryInternal();

      if (nextKey == null)
        return null;

      Set<OIdentifiable> result;
      do {
        result = calculateTxValue(nextKey, indexChanges);
        key = nextKey;

        nextKey = indexChanges.getLowerKey(nextKey);

        if (nextKey != null && ODefaultComparator.INSTANCE.compare(nextKey, firstKey) < 0)
          nextKey = null;
      } while ((result == null || result.isEmpty()) && nextKey != null);

      if (result == null || result.isEmpty())
        return null;

      valuesIterator = result.iterator();
      return nextEntryInternal();
    }

    private Map.Entry<Object, OIdentifiable> nextEntryInternal() {
      final OIdentifiable identifiable = valuesIterator.next();
      return new Map.Entry<Object, OIdentifiable>() {
        @Override
        public Object getKey() {
          return key;
        }

        @Override
        public OIdentifiable getValue() {
          return identifiable;
        }

        @Override
        public OIdentifiable setValue(OIdentifiable value) {
          throw new UnsupportedOperationException("setValue");
        }
      };
    }
  }

  private class OIndexTxCursor extends OIndexAbstractCursor {

    private final OIndexCursor             backedCursor;
    private final boolean                  ascOrder;
    private final OTransactionIndexChanges indexChanges;
    private       OIndexCursor             txBetweenIndexCursor;

    private Map.Entry<Object, OIdentifiable> nextTxEntry;
    private Map.Entry<Object, OIdentifiable> nextBackedEntry;

    private boolean firstTime;

    public OIndexTxCursor(OIndexCursor txCursor, OIndexCursor backedCursor, boolean ascOrder,
        OTransactionIndexChanges indexChanges) {
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

  public OIndexTxAwareMultiValue(final ODatabaseDocumentInternal database, final OIndex<Set<OIdentifiable>> delegate) {
    super(database, delegate);
  }

  @Override
  public Set<OIdentifiable> get(Object key) {
    final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());
    if (indexChanges == null)
      return super.get(key);

    key = getCollatingValue(key);

    final Set<OIdentifiable> result = new HashSet<OIdentifiable>();
    if (!indexChanges.cleared) {
      // BEGIN FROM THE UNDERLYING RESULT SET
      final Collection<OIdentifiable> subResult = super.get(key);
      if (subResult != null)
        for (OIdentifiable oid : subResult)
          result.add(oid);
    }

    final Set<OIdentifiable> processed = new HashSet<OIdentifiable>();
    for (OIdentifiable identifiable : result) {
      Map.Entry<Object, OIdentifiable> entry = calculateTxIndexEntry(key, identifiable, indexChanges);
      if (entry != null)
        processed.add(entry.getValue());
    }

    Set<OIdentifiable> txChanges = calculateTxValue(key, indexChanges);
    if (txChanges != null)
      processed.addAll(txChanges);

    if (!processed.isEmpty())
      return processed;

    return null;
  }

  @Override
  public boolean contains(final Object key) {
    final Set<OIdentifiable> result = get(key);
    return result != null && !result.isEmpty();
  }

  @Override
  public OIndexCursor iterateEntriesBetween(Object fromKey, final boolean fromInclusive, Object toKey, final boolean toInclusive,
      final boolean ascOrder) {

    final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());
    if (indexChanges == null)
      return super.iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascOrder);

    fromKey = getCollatingValue(fromKey);
    toKey = getCollatingValue(toKey);

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

    fromKey = getCollatingValue(fromKey);

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

    toKey = getCollatingValue(toKey);

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

    final List<Object> sortedKeys = new ArrayList<Object>(keys.size());
    for (Object key : keys)
      sortedKeys.add(getCollatingValue(key));
    if (ascSortOrder)
      Collections.sort(sortedKeys, ODefaultComparator.INSTANCE);
    else
      Collections.sort(sortedKeys, Collections.reverseOrder(ODefaultComparator.INSTANCE));

    final OIndexCursor txCursor = new OIndexAbstractCursor() {
      private Iterator<Object> keysIterator = sortedKeys.iterator();

      private Iterator<OIdentifiable> valuesIterator = new OEmptyIterator<OIdentifiable>();
      private Object key;

      @Override
      public Map.Entry<Object, OIdentifiable> nextEntry() {
        if (valuesIterator.hasNext())
          return nextEntryInternal();

        if (keysIterator == null)
          return null;

        Set<OIdentifiable> result = null;

        while (result == null && keysIterator.hasNext()) {
          key = keysIterator.next();
          result = calculateTxValue(key, indexChanges);

          if (result != null && result.isEmpty())
            result = null;
        }

        if (result == null) {
          keysIterator = null;
          return null;
        }

        valuesIterator = result.iterator();
        return nextEntryInternal();
      }

      private Map.Entry<Object, OIdentifiable> nextEntryInternal() {
        final OIdentifiable identifiable = valuesIterator.next();
        return new Map.Entry<Object, OIdentifiable>() {
          @Override
          public Object getKey() {
            return key;
          }

          @Override
          public OIdentifiable getValue() {
            return identifiable;
          }

          @Override
          public OIdentifiable setValue(OIdentifiable value) {
            throw new UnsupportedOperationException("setValue");
          }
        };
      }
    };

    if (indexChanges.cleared)
      return txCursor;

    final OIndexCursor backedCursor = super.iterateEntries(keys, ascSortOrder);
    return new OIndexTxCursor(txCursor, backedCursor, ascSortOrder, indexChanges);
  }

  private Map.Entry<Object, OIdentifiable> calculateTxIndexEntry(final Object key, final OIdentifiable backendValue,
      OTransactionIndexChanges indexChanges) {
    final OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerKey(key);
    if (changesPerKey.entries.isEmpty())
      return createMapEntry(key, backendValue);

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

    if (putCounter <= 0)
      return null;

    return createMapEntry(key, backendValue);
  }

  private Map.Entry<Object, OIdentifiable> createMapEntry(final Object key, final OIdentifiable backendValue) {
    return new MapEntry(key, backendValue);
  }

  private Set<OIdentifiable> calculateTxValue(final Object key, OTransactionIndexChanges indexChanges) {
    final OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerKey(key);
    if (changesPerKey.entries.isEmpty())
      return null;

    final List<OIdentifiable> result = new ArrayList<OIdentifiable>();
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

    return new HashSet<OIdentifiable>(result);
  }
}

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

import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.mvrbtree.OMVRBTree;
import com.orientechnologies.orient.core.record.ORecord;
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
public abstract class OIndexTxAware<T> extends OIndexAbstractDelegate<T> {
  private static final OAlwaysLessKey    ALWAYS_LESS_KEY    = new OAlwaysLessKey();
  private static final OAlwaysGreaterKey ALWAYS_GREATER_KEY = new OAlwaysGreaterKey();

  protected ODatabaseDocumentInternal    database;

  public OIndexTxAware(final ODatabaseDocumentInternal iDatabase, final OIndex<T> iDelegate) {
    super(iDelegate);
    database = iDatabase;
  }

  @Override
  public long getSize() {
    long tot = delegate.getSize();

    final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());
    if (indexChanges != null) {
      if (indexChanges.cleared)
        // BEGIN FROM 0
        tot = 0;

      for (final Entry<Object, OTransactionIndexChangesPerKey> entry : indexChanges.changesPerKey.entrySet()) {
        for (final OTransactionIndexEntry e : entry.getValue().entries) {
          if (e.operation == OPERATION.REMOVE) {
            if (e.value == null)
              // KEY REMOVED
              tot--;
          } else if (e.operation == OPERATION.PUT) {
          }
        }
      }

      for (final OTransactionIndexEntry e : indexChanges.nullKeyChanges.entries) {
        if (e.operation == OPERATION.REMOVE) {
          if (e.value == null)
            // KEY REMOVED
            tot--;
        } else if (e.operation == OPERATION.PUT) {
        }
      }
    }

    return tot;
  }

  @Override
  public OIndexTxAware<T> put(final Object iKey, final OIdentifiable iValue) {
    final ORID rid = iValue.getIdentity();

    if (!rid.isValid())
      if (iValue instanceof ORecord)
        // EARLY SAVE IT
        ((ORecord) iValue).save();
      else
        throw new IllegalArgumentException("Cannot store non persistent RID as index value for key '" + iKey + "'");

    database.getTransaction().addIndexEntry(delegate, super.getName(), OPERATION.PUT, iKey, iValue);
    return this;
  }

  @Override
  public boolean remove(final Object key) {
    database.getTransaction().addIndexEntry(delegate, super.getName(), OPERATION.REMOVE, key, null);
    return true;
  }

  @Override
  public boolean remove(final Object iKey, final OIdentifiable iRID) {
    database.getTransaction().addIndexEntry(delegate, super.getName(), OPERATION.REMOVE, iKey, iRID);
    return true;
  }

  @Override
  public OIndexTxAware<T> clear() {
    database.getTransaction().addIndexEntry(delegate, super.getName(), OPERATION.CLEAR, null, null);
    return this;
  }

  @Override
  public Object getFirstKey() {
    final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());
    if (indexChanges == null)
      return delegate.getFirstKey();

    Object indexFirstKey;
    if (indexChanges.cleared)
      indexFirstKey = null;
    else
      indexFirstKey = delegate.getFirstKey();

    Object firstKey = indexChanges.getFirstKey();
    while (true) {
      OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerKey(firstKey);

      for (OTransactionIndexEntry indexEntry : changesPerKey.entries) {
        if (indexEntry.operation.equals(OPERATION.REMOVE))
          firstKey = null;
        else
          firstKey = changesPerKey.key;
      }

      if (changesPerKey.key.equals(indexFirstKey))
        indexFirstKey = firstKey;

      if (firstKey != null) {
        if (indexFirstKey != null && ((Comparable) indexFirstKey).compareTo(firstKey) < 0)
          return indexFirstKey;

        return firstKey;
      }

      firstKey = indexChanges.getHigherKey(changesPerKey.key);
      if (firstKey == null)
        return indexFirstKey;
    }
  }

  @Override
  public Object getLastKey() {
    final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());
    if (indexChanges == null)
      return delegate.getLastKey();

    Object indexLastKey;
    if (indexChanges.cleared)
      indexLastKey = null;
    else
      indexLastKey = delegate.getLastKey();

    Object lastKey = indexChanges.getLastKey();
    while (true) {
      OTransactionIndexChangesPerKey changesPerKey = indexChanges.getChangesPerKey(lastKey);

      for (OTransactionIndexEntry indexEntry : changesPerKey.entries) {
        if (indexEntry.operation.equals(OPERATION.REMOVE))
          lastKey = null;
        else
          lastKey = changesPerKey.key;
      }

      if (changesPerKey.key.equals(indexLastKey))
        indexLastKey = lastKey;

      if (lastKey != null) {
        if (indexLastKey != null && ((Comparable) indexLastKey).compareTo(lastKey) > 0)
          return indexLastKey;

        return lastKey;
      }

      lastKey = indexChanges.getLowerKey(changesPerKey.key);
      if (lastKey == null)
        return indexLastKey;
    }
  }

  protected Object enhanceCompositeKey(Object key, OMVRBTree.PartialSearchMode partialSearchMode) {
    if (!(key instanceof OCompositeKey))
      return key;

    final OCompositeKey compositeKey = (OCompositeKey) key;
    final int keySize = getDefinition().getParamCount();

    if (!(keySize == 1 || compositeKey.getKeys().size() == keySize || partialSearchMode.equals(OMVRBTree.PartialSearchMode.NONE))) {
      final OCompositeKey fullKey = new OCompositeKey(compositeKey);
      int itemsToAdd = keySize - fullKey.getKeys().size();

      final Comparable<?> keyItem;
      if (partialSearchMode.equals(OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY))
        keyItem = ALWAYS_GREATER_KEY;
      else
        keyItem = ALWAYS_LESS_KEY;

      for (int i = 0; i < itemsToAdd; i++)
        fullKey.addKey(keyItem);

      return fullKey;
    }

    return key;
  }

  protected Object enhanceToCompositeKeyBetweenAsc(Object keyTo, boolean toInclusive) {
    OMVRBTree.PartialSearchMode partialSearchModeTo;
    if (toInclusive)
      partialSearchModeTo = OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY;
    else
      partialSearchModeTo = OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY;

    keyTo = enhanceCompositeKey(keyTo, partialSearchModeTo);
    return keyTo;
  }

  protected Object enhanceFromCompositeKeyBetweenAsc(Object keyFrom, boolean fromInclusive) {
    OMVRBTree.PartialSearchMode partialSearchModeFrom;
    if (fromInclusive)
      partialSearchModeFrom = OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY;
    else
      partialSearchModeFrom = OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY;

    keyFrom = enhanceCompositeKey(keyFrom, partialSearchModeFrom);
    return keyFrom;
  }

  protected Object enhanceToCompositeKeyBetweenDesc(Object keyTo, boolean toInclusive) {
    OMVRBTree.PartialSearchMode partialSearchModeTo;
    if (toInclusive)
      partialSearchModeTo = OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY;
    else
      partialSearchModeTo = OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY;

    keyTo = enhanceCompositeKey(keyTo, partialSearchModeTo);
    return keyTo;
  }

  protected Object enhanceFromCompositeKeyBetweenDesc(Object keyFrom, boolean fromInclusive) {
    OMVRBTree.PartialSearchMode partialSearchModeFrom;
    if (fromInclusive)
      partialSearchModeFrom = OMVRBTree.PartialSearchMode.LOWEST_BOUNDARY;
    else
      partialSearchModeFrom = OMVRBTree.PartialSearchMode.HIGHEST_BOUNDARY;

    keyFrom = enhanceCompositeKey(keyFrom, partialSearchModeFrom);
    return keyFrom;
  }
}

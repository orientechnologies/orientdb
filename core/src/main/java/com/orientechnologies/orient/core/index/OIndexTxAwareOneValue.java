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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
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
    final OIdentifiable previousRecord = get(iKey);
    if (previousRecord != null && !previousRecord.equals(iRecord))
      throw new ORecordDuplicatedException(String.format(
          "Cannot index record %s: found duplicated key '%s' in index '%s' previously assigned to the record %s", iRecord, iKey,
          getName(), previousRecord), previousRecord.getIdentity());

    super.checkEntry(iRecord, iKey);
  }

  @Override
  public OIdentifiable get(final Object iKey) {
    final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());

    OIdentifiable result;
    if (indexChanges == null || !indexChanges.cleared)
      // BEGIN FROM THE UNDERLYING RESULT SET
      result = super.get(iKey);
    else
      // BEGIN FROM EMPTY RESULT SET
      result = null;

    // FILTER RESULT SET WITH TRANSACTIONAL CHANGES
    return filterIndexChanges(indexChanges, iKey, result, null);
  }

  @Override
  public boolean contains(final Object iKey) {
    final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());

    OIdentifiable result;
    if (indexChanges == null || !indexChanges.cleared)
      // BEGIN FROM THE UNDERLYING RESULT SET
      result = (OIdentifiable) super.get(iKey);
    else
      // BEGIN FROM EMPTY RESULT SET
      result = null;

    // FILTER RESULT SET WITH TRANSACTIONAL CHANGES
    return filterIndexChanges(indexChanges, iKey, result, null) != null;
  }

  @Override
  public Collection<OIdentifiable> getValues(final Collection<?> iKeys) {
    final Collection<?> keys = new ArrayList<Object>(iKeys);
    final Set<OIdentifiable> result = new HashSet<OIdentifiable>();
    final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());
    if (indexChanges == null) {
      result.addAll(super.getValues(keys));
      return result;
    }

    final Set<Object> keysToRemove = new HashSet<Object>();
    for (final Object key : keys) {
      if (indexChanges.cleared)
        keysToRemove.add(key);

      final OIdentifiable keyResult = filterIndexChanges(indexChanges, key, null, keysToRemove);

      if (keyResult != null)
        result.add(keyResult);
    }

    keys.removeAll(keysToRemove);

    if (!keys.isEmpty())
      result.addAll(super.getValues(keys));
    return result;
  }

  @Override
  public Collection<ODocument> getEntries(final Collection<?> iKeys) {
    final Collection<?> keys = new ArrayList<Object>(iKeys);
    final Set<ODocument> result = new ODocumentFieldsHashSet();
    final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());
    if (indexChanges == null) {
      result.addAll(super.getEntries(keys));
      return result;
    }

    final Set<Object> keysToRemove = new HashSet<Object>();
    for (final Object key : keys) {
      if (indexChanges.cleared)
        keysToRemove.add(key);

      final OIdentifiable keyResult = filterIndexChanges(indexChanges, key, null, keysToRemove);

      if (keyResult != null) {
        final ODocument document = new ODocument();
        document.field("key", key);
        document.field("rid", keyResult.getIdentity());
        document.unsetDirty();
        result.add(document);
      }
    }

    keys.removeAll(keysToRemove);

    if (!keys.isEmpty())
      result.addAll(super.getEntries(keys));
    return result;
  }

  protected OIdentifiable filterIndexChanges(final OTransactionIndexChanges indexChanges, final Object key, OIdentifiable iValue,
      final Set<Object> keysToRemove) {
    if (indexChanges == null)
      return iValue;

    OIdentifiable keyResult = iValue;
    // CHECK FOR THE RECEIVED KEY
    if (indexChanges.containsChangesPerKey(key)) {
      final OTransactionIndexChangesPerKey value = indexChanges.getChangesPerKey(key);
      if (value != null) {
        for (final OTransactionIndexEntry entry : value.entries) {
          if (entry.operation == OPERATION.REMOVE) {
            if (entry.value == null || entry.value.equals(keyResult)) {
              // REMOVE THE ENTIRE KEY, SO RESULT SET IS EMPTY
              if (keysToRemove != null)
                keysToRemove.add(key);
              keyResult = null;
            }
          } else if (entry.operation == OPERATION.PUT) {
            // ADD ALSO THIS RID
            if (keysToRemove != null)
              keysToRemove.add(key);
            keyResult = entry.value;
          }
        }
      }
    }

    // CHECK FOR ANY KEYS
    if (indexChanges.containsChangesCrossKey()) {
      final OTransactionIndexChangesPerKey value = indexChanges.getChangesCrossKey();
      if (value != null) {
        for (final OTransactionIndexEntry entry : value.entries) {
          if (entry.operation == OPERATION.REMOVE) {
            if (entry.value == null || entry.value.equals(keyResult)) {
              // REMOVE THE ENTIRE KEY, SO RESULT SET IS EMPTY
              if (keysToRemove != null)
                keysToRemove.add(key);
              keyResult = null;
            }
          } else if (entry.operation == OPERATION.PUT) {
            // ADD ALSO THIS RID
            if (keysToRemove != null)
              keysToRemove.add(key);
            keyResult = entry.value;
          }
        }
      }
    }
    return keyResult;
  }
}

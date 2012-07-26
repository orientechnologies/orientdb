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
import java.util.TreeSet;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
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
public class OIndexTxAwareMultiValue extends OIndexTxAware<Collection<OIdentifiable>> {
  public OIndexTxAwareMultiValue(final ODatabaseRecord iDatabase, final OIndex<Collection<OIdentifiable>> iDelegate) {
    super(iDatabase, iDelegate);
  }

  @Override
  public Collection<OIdentifiable> get(final Object iKey) {

    final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());

    final Set<OIdentifiable> result = new TreeSet<OIdentifiable>();
    if (indexChanges == null || !indexChanges.cleared) {
      // BEGIN FROM THE UNDERLYING RESULT SET
      final Collection<OIdentifiable> subResult = super.get(iKey);
      if (subResult != null)
        for (OIdentifiable oid : subResult)
          if (oid != null)
            result.add(oid);
    }

    return filterIndexChanges(indexChanges, iKey, result);
  }

  @Override
  public boolean contains(final Object iKey) {
    final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());

    Set<OIdentifiable> result = new TreeSet<OIdentifiable>();
    if (indexChanges == null || !indexChanges.cleared) {
      // BEGIN FROM THE UNDERLYING RESULT SET
      final Collection<OIdentifiable> subResult = super.get(iKey);
      if (subResult != null)
        for (OIdentifiable oid : subResult)
          if (oid != null)
            result.add(oid);
    }

    filterIndexChanges(indexChanges, iKey, result);
    return !result.isEmpty();
  }

  @Override
  public Collection<OIdentifiable> getValues(final Collection<?> iKeys) {
    final Collection<?> keys = new ArrayList<Object>(iKeys);

    final Set<OIdentifiable> result = new TreeSet<OIdentifiable>();

    final Set<Object> keysToRemove = new TreeSet<Object>();
    final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());

    if (indexChanges == null) {
      result.addAll(super.getValues(keys));
      return result;
    }

    for (final Object key : keys) {
      final Set<OIdentifiable> keyResult;
      if (!indexChanges.cleared)
        if (indexChanges.containsChangesPerKey(key))
          // BEGIN FROM THE UNDERLYING RESULT SET
          keyResult = new TreeSet<OIdentifiable>(super.get(key));
        else
          continue;
      else
        // BEGIN FROM EMPTY RESULT SET
        keyResult = new TreeSet<OIdentifiable>();

      keysToRemove.add(key);
      filterIndexChanges(indexChanges, key, keyResult);
      result.addAll(keyResult);
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

    final Set<Object> keysToRemove = new HashSet<Object>();
    final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());

    if (indexChanges == null) {
      return super.getEntries(keys);
    }

    for (final Object key : keys) {
      final Set<OIdentifiable> keyResult;
      if (!indexChanges.cleared)
        if (indexChanges.containsChangesPerKey(key))
          // BEGIN FROM THE UNDERLYING RESULT SET
          keyResult = new TreeSet<OIdentifiable>(super.get(key));
        else
          continue;
      else
        // BEGIN FROM EMPTY RESULT SET
        keyResult = new TreeSet<OIdentifiable>();

      keysToRemove.add(key);

      filterIndexChanges(indexChanges, key, keyResult);

      for (final OIdentifiable id : keyResult) {
        final ODocument document = new ODocument();
        document.field("key", key);
        document.field("rid", id.getIdentity());
        document.unsetDirty();
        result.add(document);
      }
    }

    keys.removeAll(keysToRemove);

    if (!keys.isEmpty())
      result.addAll(super.getEntries(keys));
    return result;
  }

  protected Collection<OIdentifiable> filterIndexChanges(final OTransactionIndexChanges indexChanges, final Object key,
      final Collection<OIdentifiable> keyResult) {
    if (indexChanges == null)
      return keyResult;

    if (indexChanges.containsChangesPerKey(key)) {
      final OTransactionIndexChangesPerKey value = indexChanges.getChangesPerKey(key);
      if (value != null) {
        for (final OTransactionIndexEntry entry : value.entries) {
          if (entry.operation == OPERATION.REMOVE) {
            if (entry.value == null) {
              // REMOVE THE ENTIRE KEY, SO RESULT SET IS EMPTY
              keyResult.clear();
              break;
            } else
              // REMOVE ONLY THIS RID
              keyResult.remove(entry.value);
          } else if (entry.operation == OPERATION.PUT) {
            // ADD ALSO THIS RID
            keyResult.add(entry.value);
          }
        }
      }
    }

    if (indexChanges.containsChangesCrossKey()) {
      final OTransactionIndexChangesPerKey value = indexChanges.getChangesCrossKey();
      if (value != null) {
        for (final OTransactionIndexEntry entry : value.entries) {
          if (entry.operation == OPERATION.REMOVE) {
            if (entry.value == null) {
              // REMOVE THE ENTIRE KEY, SO RESULT SET IS EMPTY
              keyResult.clear();
              break;
            } else
              // REMOVE ONLY THIS RID
              keyResult.remove(entry.value);
          } else if (entry.operation == OPERATION.PUT) {
            // ADD ALSO THIS RID
            keyResult.add(entry.value);
          }
        }
      }
    }
    return keyResult;
  }
}

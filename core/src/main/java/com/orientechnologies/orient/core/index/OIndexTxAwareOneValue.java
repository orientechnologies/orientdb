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
import com.orientechnologies.orient.core.metadata.schema.OType;
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
    return filterIndexChanges(indexChanges, Collections.singletonMap(key, result), null).get(key);
  }

  @Override
  public boolean contains(final Object key) {
    final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());
    if (indexChanges == null)
      return super.get(key) != null;

    OIdentifiable result;
    if (!indexChanges.cleared)
      // BEGIN FROM THE UNDERLYING RESULT SET
      result = (OIdentifiable) super.get(key);
    else
      // BEGIN FROM EMPTY RESULT SET
      result = null;

    // FILTER RESULT SET WITH TRANSACTIONAL CHANGES
    return filterIndexChanges(indexChanges, Collections.singletonMap(key, result), null).get(key) != null;
  }

  @Override
  public Collection<OIdentifiable> getValues(final Collection<?> iKeys, boolean ascSortOrder) {
    final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());

    if (indexChanges == null)
      return super.getValues(iKeys, ascSortOrder);

    final Comparator<Object> comparator;
    if (ascSortOrder)
      comparator = ODefaultComparator.INSTANCE;
    else
      comparator = Collections.reverseOrder(ODefaultComparator.INSTANCE);

    final TreeMap<Object, OIdentifiable> result = new TreeMap<Object, OIdentifiable>(comparator);

    final Collection<?> keys = new ArrayList<Object>(iKeys);

    final Set<Object> keysToRemove = new HashSet<Object>();
    final Map<Object, OIdentifiable> keyValueEntries = new HashMap<Object, OIdentifiable>();

    for (final Object key : keys) {
      if (indexChanges.cleared)
        keysToRemove.add(key);

      keyValueEntries.put(key, null);
    }

    final Map<Object, OIdentifiable> keyResult = filterIndexChanges(indexChanges, keyValueEntries, keysToRemove);

    for (Map.Entry<Object, OIdentifiable> keyResultEntry : keyResult.entrySet())
      result.put(keyResultEntry.getKey(), keyResultEntry.getValue().getIdentity());

    keys.removeAll(keysToRemove);

    if (!keys.isEmpty()) {
      final Collection<ODocument> entries = super.getEntries(keys);

      for (ODocument entry : entries)
        result.put(entry.field("key"), entry.<OIdentifiable> field("rid", OType.LINK));
    }

    return result.values();
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
    final Map<Object, OIdentifiable> keyValueEntries = new HashMap<Object, OIdentifiable>();

    for (final Object key : keys) {
      if (indexChanges.cleared)
        keysToRemove.add(key);

      keyValueEntries.put(key, null);
    }

    final Map<Object, OIdentifiable> keyResult = filterIndexChanges(indexChanges, keyValueEntries, keysToRemove);

    for (Map.Entry<Object, OIdentifiable> keyResultEntry : keyResult.entrySet()) {
      final ODocument document = new ODocument();
      document.field("key", keyResultEntry.getKey());
      document.field("rid", keyResultEntry.getValue().getIdentity());

      document.unsetDirty();
      result.add(document);
    }

    keys.removeAll(keysToRemove);

    if (!keys.isEmpty())
      result.addAll(super.getEntries(keys));

    return result;
  }

  protected Map<Object, OIdentifiable> filterIndexChanges(OTransactionIndexChanges indexChanges,
      Map<Object, OIdentifiable> keyValueEntries, final Set<Object> keysToRemove) {
    final Map<Object, OIdentifiable> result = new HashMap<Object, OIdentifiable>();
    for (Map.Entry<Object, OIdentifiable> keyValueEntry : keyValueEntries.entrySet()) {
      OIdentifiable keyResult = keyValueEntry.getValue();
      Object key = keyValueEntry.getKey();

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

      if (keyResult != null)
        result.put(key, keyResult.getIdentity());
    }

    return result;
  }

  @Override
  public Collection<ODocument> getEntriesBetween(Object rangeFrom, Object rangeTo) {
    final OTransactionIndexChanges indexChanges = database.getTransaction().getIndexChanges(delegate.getName());
    if (indexChanges == null)
      return super.getEntriesBetween(rangeFrom, rangeTo);

    final OIndexDefinition indexDefinition = getDefinition();
    Object compRangeFrom = rangeFrom;
    Object compRangeTo = rangeTo;
    if (indexDefinition instanceof OCompositeIndexDefinition || indexDefinition.getParamCount() > 1) {
      int keySize = indexDefinition.getParamCount();

      final OCompositeKey fullKeyFrom = new OCompositeKey((Comparable) rangeFrom);
      final OCompositeKey fullKeyTo = new OCompositeKey((Comparable) rangeTo);

      while (fullKeyFrom.getKeys().size() < keySize)
        fullKeyFrom.addKey(new OAlwaysLessKey());

      while (fullKeyTo.getKeys().size() < keySize)
        fullKeyTo.addKey(new OAlwaysGreaterKey());

      compRangeFrom = fullKeyFrom;
      compRangeTo = fullKeyTo;
    }

    final Collection<OTransactionIndexChangesPerKey> rangeChanges = indexChanges.getChangesForKeys(compRangeFrom, compRangeTo);
    if (rangeChanges.isEmpty())
      return super.getEntriesBetween(rangeFrom, rangeTo);

    final Map<Object, OIdentifiable> keyValueEntries = new HashMap<Object, OIdentifiable>();
    if (indexChanges.cleared) {
      for (OTransactionIndexChangesPerKey changesPerKey : rangeChanges)
        keyValueEntries.put(changesPerKey.key, null);
    } else {
      final Collection<ODocument> storedEntries = super.getEntriesBetween(rangeFrom, rangeTo);
      for (ODocument entry : storedEntries)
        keyValueEntries.put(entry.field("key"), entry.<OIdentifiable> field("rid"));

      for (OTransactionIndexChangesPerKey changesPerKey : rangeChanges)
        if (!keyValueEntries.containsKey(changesPerKey.key))
          keyValueEntries.put(changesPerKey.key, null);
    }

    final Map<Object, OIdentifiable> keyValuesResult = filterIndexChanges(indexChanges, keyValueEntries, null);

    final Set<ODocument> result = new ODocumentFieldsHashSet();
    for (Map.Entry<Object, OIdentifiable> keyResultEntry : keyValuesResult.entrySet()) {
      final ODocument document = new ODocument();
      document.field("key", keyResultEntry.getKey());
      document.field("rid", keyResultEntry.getValue().getIdentity());

      document.unsetDirty();
      result.add(document);
    }

    return result;
  }
}

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

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;

/**
 * Index implementation that allows only one value for a key.
 *
 * @author Luca Garulli
 */
public class OIndexUnique extends OIndexOneValue {

  private final OIndexEngine.Validator<Object, OIdentifiable> UNIQUE_VALIDATOR = new OIndexEngine.Validator<Object, OIdentifiable>() {
    @Override
    public Object validate(Object key, OIdentifiable oldValue, OIdentifiable newValue) {
      if (oldValue != null) {
        // CHECK IF THE ID IS THE SAME OF CURRENT: THIS IS THE UPDATE CASE
        if (!oldValue.equals(newValue)) {
          final Boolean mergeSameKey = metadata != null ? (Boolean) metadata.field(OIndex.MERGE_KEYS) : Boolean.FALSE;
          if (mergeSameKey == null || !mergeSameKey)
            throw new ORecordDuplicatedException(String
                .format("Cannot index record %s: found duplicated key '%s' in index '%s' previously assigned to the record %s",
                    newValue.getIdentity(), key, getName(), oldValue.getIdentity()), getName(), oldValue.getIdentity());
        } else
          return OIndexEngine.Validator.IGNORE;
      }

      if (!newValue.getIdentity().isPersistent())
        newValue.getRecord().save();
      return newValue.getIdentity();
    }
  };

  public OIndexUnique(String name, String typeId, String algorithm, int version, OAbstractPaginatedStorage storage,
      String valueContainerAlgorithm, ODocument metadata) {
    super(name, typeId, algorithm, version, storage, valueContainerAlgorithm, metadata);
  }

  @Override
  public OIndexOneValue put(Object key, final OIdentifiable iSingleValue) {
    if (iSingleValue != null && !iSingleValue.getIdentity().isPersistent())
      throw new IllegalArgumentException("Cannot index a non persistent record (" + iSingleValue.getIdentity() + ")");

    key = getCollatingValue(key);

    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();

    if (!txIsActive) {
      keyLockManager.acquireExclusiveLock(key);
    }

    try {
      acquireSharedLock();
      try {
        while (true)
          try {
            storage.validatedPutIndexValue(indexId, key, iSingleValue, UNIQUE_VALIDATOR);
            break;
          } catch (OInvalidIndexEngineIdException e) {
            doReloadIndexEngine();
          }
        return this;
      } finally {
        releaseSharedLock();
      }
    } finally {
      if (!txIsActive)
        keyLockManager.releaseExclusiveLock(key);
    }
  }

  @Override
  public boolean canBeUsedInEqualityOperators() {
    return true;
  }

  @Override
  public boolean supportsOrderedIterations() {
    while (true)
      try {
        return storage.hasIndexRangeQuerySupport(indexId);
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
  }

  @Override
  protected Iterable<OTransactionIndexChangesPerKey.OTransactionIndexEntry> interpretTxKeyChanges(
      OTransactionIndexChangesPerKey changes) {
    return changes.interpret(OTransactionIndexChangesPerKey.Interpretation.Unique);
  }
}

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
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;

/**
 * Index implementation that allows only one value for a key.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexUnique extends OIndexOneValue {
  public OIndexUnique(String name, String typeId, String algorithm, OIndexEngine<OIdentifiable> engine,
      String valueContainerAlgorithm, ODocument metadata) {
    super(name, typeId, algorithm, engine, valueContainerAlgorithm, metadata);
  }

  @Override
  public OIndexOneValue put(Object key, final OIdentifiable iSingleValue) {
    checkForRebuild();

    key = getCollatingValue(key);

    final ODatabase database = getDatabase();
    final boolean txIsActive = database.getTransaction().isActive();

    if (!txIsActive)
      keyLockManager.acquireExclusiveLock(key);

    try {
      modificationLock.requestModificationLock();
      try {
        checkForKeyType(key);
        acquireSharedLock();
        try {
          final OIdentifiable value = indexEngine.get(key);

          if (value != null) {
            // CHECK IF THE ID IS THE SAME OF CURRENT: THIS IS THE UPDATE CASE
            if (!value.equals(iSingleValue)) {
              final Boolean mergeSameKey = metadata != null ? (Boolean) metadata.field(OIndex.MERGE_KEYS) : Boolean.FALSE;
              if (mergeSameKey != null && mergeSameKey)
                // IGNORE IT, THE EXISTENT KEY HAS BEEN MERGED
                ;
              else
                throw new ORecordDuplicatedException(String.format(
                    "Cannot index record %s: found duplicated key '%s' in index '%s' previously assigned to the record %s",
                    iSingleValue.getIdentity(), key, getName(), value.getIdentity()), value.getIdentity());
            } else
              return this;
          }

          if (!iSingleValue.getIdentity().isPersistent())
            ((ORecord) iSingleValue.getRecord()).save();

          markStorageDirty();

          indexEngine.put(key, iSingleValue.getIdentity());
          return this;

        } finally {
          releaseSharedLock();
        }
      } finally {
        modificationLock.releaseModificationLock();
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
    return indexEngine.hasRangeQuerySupport();
  }
}

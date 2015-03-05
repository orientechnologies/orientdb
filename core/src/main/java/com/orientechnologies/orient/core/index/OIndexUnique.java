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

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Index implementation that allows only one value for a key.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexUnique extends OIndexOneValue {
  public OIndexUnique(String typeId, String algorithm, OIndexEngine<OIdentifiable> engine, String valueContainerAlgorithm,
      ODocument metadata) {
    super(typeId, algorithm, engine, valueContainerAlgorithm, metadata);
  }

  @Override
  public OIndexOneValue put(Object key, final OIdentifiable iSingleValue) {
    checkForRebuild();

    key = getCollatingValue(key);

		final ODatabase database = getDatabase();
		final boolean txIsActive = database.getTransaction().isActive();

		if (txIsActive) {
                    keyLockManager.acquireSharedLock(key);
    }

    try {
      modificationLock.requestModificationLock();
      try {
        checkForKeyType(key);
        acquireExclusiveLock();
        try {
          final OIdentifiable value = indexEngine.get(key);

          if (value != null) {
            // CHECK IF THE ID IS THE SAME OF CURRENT: THIS IS THE UPDATE CASE
            if (!value.equals(iSingleValue)) {
              final Boolean mergeSameKey = metadata != null ? (Boolean) metadata.field(OIndex.MERGE_KEYS) : Boolean.FALSE;
              if (mergeSameKey != null && mergeSameKey)
                // IGNORE IT, THE EXISTENT KEY HAS BEEN MERGED
                ;
              else {
                  throw new ORecordDuplicatedException(String.format(
                          "Cannot index record %s: found duplicated key '%s' in index '%s' previously assigned to the record %s",
                          iSingleValue.getIdentity(), key, getName(), value.getIdentity()), value.getIdentity());
              }
            } else {
                return this;
            }
          }

          if (!iSingleValue.getIdentity().isPersistent()) {
              ((ORecord) iSingleValue.getRecord()).save();
          }

          markStorageDirty();

          indexEngine.put(key, iSingleValue.getIdentity());
          return this;

        } finally {
          releaseExclusiveLock();
        }
      } finally {
        modificationLock.releaseModificationLock();
      }

    } finally {
			if (txIsActive) {
                            keyLockManager.releaseSharedLock(key);
                        }
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

  @Override
  protected void putInSnapshot(Object key, OIdentifiable value, Map<Object, Object> snapshot) {
    key = getCollatingValue(key);

    Object snapshotValue = snapshot.get(key);
    if (snapshotValue == null) {
      final OIdentifiable storedValue = indexEngine.get(key);

      final Set<OIdentifiable> values = new LinkedHashSet<OIdentifiable>();

      if (storedValue != null) {
          values.add(storedValue.getIdentity());
      }

      values.add(value.getIdentity());

      snapshot.put(key, values);
    } else if (snapshotValue instanceof Set) {
      final Set<OIdentifiable> values = (Set<OIdentifiable>) snapshotValue;

      values.add(value.getIdentity());
    } else {
      final Set<OIdentifiable> values = new LinkedHashSet<OIdentifiable>();

      values.add(value);
      snapshot.put(key, values);
    }
  }

  @Override
  protected void removeFromSnapshot(Object key, OIdentifiable value, Map<Object, Object> snapshot) {
    key = getCollatingValue(key);

    Object snapshotValue = snapshot.get(key);

    if (snapshotValue instanceof Set) {
      final Set<OIdentifiable> values = (Set<OIdentifiable>) snapshotValue;
      if (values.isEmpty()) {
          snapshot.put(key, RemovedValue.INSTANCE);
      } else {
          values.remove(value);
      }
    } else {
        snapshot.put(key, RemovedValue.INSTANCE);
    }
  }

  @Override
  protected void commitSnapshot(Map<Object, Object> snapshot) {
    for (Map.Entry<Object, Object> snapshotEntry : snapshot.entrySet()) {
      Object key = snapshotEntry.getKey();
      checkForKeyType(key);

      Object snapshotValue = snapshotEntry.getValue();
      if (snapshotValue instanceof Set) {
        Set<OIdentifiable> values = (Set<OIdentifiable>) snapshotValue;
        if (values.isEmpty()) {
            continue;
        }

        final Iterator<OIdentifiable> valuesIterator = values.iterator();
        if (values.size() > 1) {
          final OIdentifiable valueOne = valuesIterator.next();
          final OIdentifiable valueTwo = valuesIterator.next();
          throw new ORecordDuplicatedException(String.format(
              "Cannot index record %s: found duplicated key '%s' in index '%s' previously assigned to the record %s",
              valueTwo.getIdentity(), key, getName(), valueOne.getIdentity()), valueOne.getIdentity());
        }

        final OIdentifiable value = valuesIterator.next();
        indexEngine.put(key, value.getIdentity());
      } else if (snapshotValue.equals(RemovedValue.INSTANCE)) {
          indexEngine.remove(key);
      } else {
          assert false : "Provided value can not be committed";
      }
    }
  }
}

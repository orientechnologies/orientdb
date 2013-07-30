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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;

/**
 * Index implementation that allows only one value for a key.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexUnique extends OIndexOneValue {
  public OIndexUnique(String typeId, OIndexEngine<OIdentifiable> engine) {
    super(typeId, engine);
  }

  public OIndexOneValue put(final Object key, final OIdentifiable iSingleValue) {
    checkForRebuild();

    modificationLock.requestModificationLock();
    try {
      acquireExclusiveLock();
      try {
        checkForKeyType(key);
        final OIdentifiable value = indexEngine.get(key);

        if (value != null) {
          // CHECK IF THE ID IS THE SAME OF CURRENT: THIS IS THE UPDATE CASE
          if (!value.equals(iSingleValue))
            throw new ORecordDuplicatedException(String.format(
                "Cannot index record %s: found duplicated key '%s' in index '%s' previously assigned to the record %s", null,
                OIndexException.class, iSingleValue.getIdentity(), key, getName(), value.getIdentity()), value.getIdentity());
          else
            return this;
        }

        if (!iSingleValue.getIdentity().isPersistent())
          ((ORecord<?>) iSingleValue.getRecord()).save();

        indexEngine.put(key, iSingleValue.getIdentity());
        return this;

      } finally {
        releaseExclusiveLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  public boolean canBeUsedInEqualityOperators() {
    return true;
  }

  @Override
  public boolean supportsOrderedIterations() {
    return indexEngine.hasRangeQuerySupport();
  }
}

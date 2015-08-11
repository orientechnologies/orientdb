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
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Dictionary index similar to unique index but does not check for updates, just executes changes. Last put always wins and override
 * the previous value.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexDictionary extends OIndexOneValue {

  public OIndexDictionary(String name, String typeId, String algorithm, OIndexEngine<OIdentifiable> engine,
      String valueContainerAlgorithm, ODocument metadata) {
    super(name, typeId, algorithm, engine, valueContainerAlgorithm, metadata);
  }

  public OIndexOneValue put(Object key, final OIdentifiable value) {

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
          markStorageDirty();
          indexEngine.put(key, value);
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

  /**
   * Disables check of entries.
   */
  @Override
  public ODocument checkEntry(final OIdentifiable record, final Object key) {
    return null;
  }

  public boolean canBeUsedInEqualityOperators() {
    return true;
  }

  public boolean supportsOrderedIterations() {
    return false;
  }
}

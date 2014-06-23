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

import java.util.Map;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Dictionary index similar to unique index but does not check for updates, just executes changes. Last put always wins and override
 * the previous value.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexDictionary extends OIndexOneValue {

  public OIndexDictionary(String typeId, String algorithm, OIndexEngine<OIdentifiable> engine, String valueContainerAlgorithm) {
    super(typeId, algorithm, engine, valueContainerAlgorithm);
  }

  public OIndexOneValue put(Object key, final OIdentifiable value) {
    modificationLock.requestModificationLock();

    key = getCollatingValue(key);

    try {
      checkForKeyType(key);
      acquireSharedLock();
      try {
        indexEngine.put(key, value);
        return this;

      } finally {
        releaseSharedLock();
      }
    } finally {
      modificationLock.releaseModificationLock();
    }
  }

  /**
   * Disables check of entries.
   */
  @Override
  public void checkEntry(final OIdentifiable iRecord, final Object key) {
  }

  public boolean canBeUsedInEqualityOperators() {
    return true;
  }

  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  protected void putInSnapshot(Object key, OIdentifiable value, Map<Object, Object> snapshot) {
    key = getCollatingValue(key);
    snapshot.put(key, value.getIdentity());
  }

  @Override
  protected void removeFromSnapshot(Object key, OIdentifiable value, Map<Object, Object> snapshot) {
    key = getCollatingValue(key);
    snapshot.put(key, RemovedValue.INSTANCE);
  }

  @Override
  protected void commitSnapshot(Map<Object, Object> snapshot) {
    for (Map.Entry<Object, Object> snapshotEntry : snapshot.entrySet()) {
      Object key = snapshotEntry.getKey();
      checkForKeyType(key);

      Object snapshotValue = snapshotEntry.getValue();
      if (snapshotValue.equals(RemovedValue.INSTANCE))
        indexEngine.remove(key);
      else
        indexEngine.put(key, (OIdentifiable) snapshotValue);
    }
  }
}

/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;

/**
 * Dictionary index similar to unique index but does not check for updates, just executes changes. Last put always wins and override
 * the previous value.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OIndexDictionary extends OIndexOneValue {

  public OIndexDictionary(String name, String typeId, String algorithm, int version, OAbstractPaginatedStorage storage,
      String valueContainerAlgorithm, ODocument metadata, int binaryFormatVersion) {
    super(name, typeId, algorithm, version, storage, valueContainerAlgorithm, metadata, binaryFormatVersion);
  }

  public OIndexOneValue put(Object key, final OIdentifiable value) {

    key = getCollatingValue(key);

    acquireSharedLock();
    try {
      if (apiVersion == 0) {
        putV0(key, value);
      } else if (apiVersion == 1) {
        putV1(key, value.getIdentity());
      } else {
        throw new IllegalStateException("Invalid API version, " + apiVersion);
      }

      return this;
    } finally {
      releaseSharedLock();
    }
  }

  private void putV0(Object key, OIdentifiable value) {
    while (true) {
      try {
        storage.putIndexValue(indexId, key, value);
        break;
      } catch (OInvalidIndexEngineIdException ignore) {
        doReloadIndexEngine();
      }
    }
  }

  private void putV1(Object key, OIdentifiable value) {
    while (true) {
      try {
        storage.putRidIndexEntry(indexId, key, value.getIdentity());
        break;
      } catch (OInvalidIndexEngineIdException ignore) {
        doReloadIndexEngine();
      }
    }
  }

  public boolean canBeUsedInEqualityOperators() {
    return true;
  }

  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  protected Iterable<OTransactionIndexChangesPerKey.OTransactionIndexEntry> interpretTxKeyChanges(
      OTransactionIndexChangesPerKey changes) {
    return changes.interpret(OTransactionIndexChangesPerKey.Interpretation.Dictionary);
  }
}

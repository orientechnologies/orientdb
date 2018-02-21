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
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;

/**
 * Dictionary index similar to unique index but does not check for updates, just executes changes. Last put always wins and override
 * the previous value.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OIndexDictionary extends OIndexOneValue {

  public OIndexDictionary(String name, String typeId, String algorithm, int version, OStorage storage,
      String valueContainerAlgorithm, ODocument metadata) {
    super(name, typeId, algorithm, version, storage, valueContainerAlgorithm, metadata);
  }

  public OIndexOneValue put(Object key, final OIdentifiable value) {

    key = getCollatingValue(key);

    acquireSharedLock();
    try {
      while (true) {
        try {
          storage.putIndexValue(indexId, key, value);
          return this;
        } catch (OInvalidIndexEngineIdException ignore) {
          doReloadIndexEngine();
        }
      }

    } finally {
      releaseSharedLock();
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

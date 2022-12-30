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

import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.engine.OBaseIndexEngine;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;

/**
 * Index implementation that allows only one value for a key.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OIndexUnique extends OIndexOneValue {

  private final OBaseIndexEngine.Validator<Object, ORID> uniqueValidator =
      (key, oldValue, newValue) -> {
        if (oldValue != null) {
          // CHECK IF THE ID IS THE SAME OF CURRENT: THIS IS THE UPDATE CASE
          if (!oldValue.equals(newValue)) {
            final Boolean mergeSameKey =
                metadata != null ? (Boolean) metadata.field(OIndex.MERGE_KEYS) : Boolean.FALSE;
            if (mergeSameKey == null || !mergeSameKey) {
              throw new ORecordDuplicatedException(
                  String.format(
                      "Cannot index record %s: found duplicated key '%s' in index '%s' previously assigned to the record %s",
                      newValue.getIdentity(), key, getName(), oldValue.getIdentity()),
                  getName(),
                  oldValue.getIdentity(),
                  key);
            }
          } else {
            return OBaseIndexEngine.Validator.IGNORE;
          }
        }

        if (!newValue.getIdentity().isPersistent()) {
          newValue = newValue.getRecord().getIdentity();
        }
        return newValue.getIdentity();
      };

  public OIndexUnique(
      String name,
      String typeId,
      String algorithm,
      int version,
      OAbstractPaginatedStorage storage,
      String valueContainerAlgorithm,
      ODocument metadata,
      int binaryFormatVersion) {
    super(
        name,
        typeId,
        algorithm,
        version,
        storage,
        valueContainerAlgorithm,
        metadata,
        binaryFormatVersion);
  }

  @Override
  public boolean isNativeTxSupported() {
    return true;
  }

  @Override
  public void doPut(OAbstractPaginatedStorage storage, Object key, ORID rid)
      throws OInvalidIndexEngineIdException {
    storage.validatedPutIndexValue(indexId, key, rid, uniqueValidator);
  }

  @Override
  public boolean canBeUsedInEqualityOperators() {
    return true;
  }

  @Override
  public boolean supportsOrderedIterations() {
    while (true) {
      try {
        return storage.hasIndexRangeQuerySupport(indexId);
      } catch (OInvalidIndexEngineIdException ignore) {
        doReloadIndexEngine();
      }
    }
  }

  @Override
  public Iterable<OTransactionIndexChangesPerKey.OTransactionIndexEntry> interpretTxKeyChanges(
      OTransactionIndexChangesPerKey changes) {
    return changes.interpret(OTransactionIndexChangesPerKey.Interpretation.Unique);
  }
}

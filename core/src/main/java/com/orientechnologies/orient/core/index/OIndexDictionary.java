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

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;

/**
 * Dictionary index similar to unique index but does not check for updates, just executes changes.
 * Last put always wins and override the previous value.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OIndexDictionary extends OIndexOneValue {

  public OIndexDictionary(
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

  public OIndexOneValue put(Object key, final OIdentifiable value) {
    key = getCollatingValue(key);

    ODatabaseDocumentInternal database = getDatabase();
    if (database.getTransaction().isActive()) {
      OTransaction singleTx = database.getTransaction();
      singleTx.addIndexEntry(
          this, super.getName(), OTransactionIndexChanges.OPERATION.PUT, key, value.getIdentity());
    } else {
      database.begin();
      OTransaction singleTx = database.getTransaction();
      singleTx.addIndexEntry(
          this, super.getName(), OTransactionIndexChanges.OPERATION.PUT, key, value.getIdentity());
      database.commit();
    }
    return this;
  }

  @Override
  public void doPut(OAbstractPaginatedStorage storage, Object key, ORID rid)
      throws OInvalidIndexEngineIdException {
    if (apiVersion == 0) {
      putV0(storage, indexId, key, rid);
    } else if (apiVersion == 1) {
      putV1(storage, indexId, key, rid);
    } else {
      throw new IllegalStateException("Invalid API version, " + apiVersion);
    }
  }

  @Override
  public boolean isNativeTxSupported() {
    return true;
  }

  private static void putV0(
      final OAbstractPaginatedStorage storage, int indexId, Object key, OIdentifiable value)
      throws OInvalidIndexEngineIdException {
    storage.putIndexValue(indexId, key, value);
  }

  private static void putV1(
      final OAbstractPaginatedStorage storage, int indexId, Object key, OIdentifiable value)
      throws OInvalidIndexEngineIdException {
    storage.putRidIndexEntry(indexId, key, value.getIdentity());
  }

  public boolean canBeUsedInEqualityOperators() {
    return true;
  }

  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public Iterable<OTransactionIndexChangesPerKey.OTransactionIndexEntry> interpretTxKeyChanges(
      OTransactionIndexChangesPerKey changes) {
    return changes.interpret(OTransactionIndexChangesPerKey.Interpretation.Dictionary);
  }
}

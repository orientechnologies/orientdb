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
package com.orientechnologies.orient.core.tx;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.ODatabase.OPERATION_MODE;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.RecordReader;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * No operation transaction.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OTransactionNoTx extends OTransactionAbstract {
  public OTransactionNoTx(
      final ODatabaseDocumentInternal iDatabase, Map<ORID, LockedRecordMetadata> noTxLocks) {
    super(iDatabase);
    if (noTxLocks != null) {
      setLocks(noTxLocks);
    }
  }

  public void begin() {}

  public void commit() {}

  @Override
  public int getEntryCount() {
    return 0;
  }

  @Override
  public void commit(boolean force) {}

  public void rollback() {}

  public ORecord loadRecord(
      final ORID iRid, final ORecord iRecord, final String iFetchPlan, final boolean ignoreCache) {
    if (iRid.isNew()) return null;

    return database.executeReadRecordNormal(
        (ORecordId) iRid, iRecord, iFetchPlan, ignoreCache, !ignoreCache);
  }

  @Override
  public ORecord reloadRecord(ORID rid, ORecord record, String fetchPlan, boolean ignoreCache) {
    return reloadRecord(rid, record, fetchPlan, ignoreCache, true);
  }

  @Override
  public ORecord reloadRecord(
      ORID rid, ORecord record, String fetchPlan, boolean ignoreCache, boolean force) {
    if (rid.isNew()) return null;

    final RecordReader recordReader;
    final ORecord loadedRecord;
    if (force) {
      loadedRecord =
          database.executeReadRecordNormal(
              (ORecordId) rid.getIdentity(), record, fetchPlan, ignoreCache, !ignoreCache);
    } else {
      loadedRecord =
          database.executeReadRecordIfLatest(
              (ORecordId) rid.getIdentity(), record, -1, fetchPlan, ignoreCache, !ignoreCache);
    }

    if (force) {
      return loadedRecord;
    } else {
      if (loadedRecord == null) return record;

      return loadedRecord;
    }
  }

  @Override
  public ORecord loadRecordIfVersionIsNotLatest(
      ORID rid, int recordVersion, String fetchPlan, boolean ignoreCache)
      throws ORecordNotFoundException {
    if (rid.isNew()) return null;
    return database.executeReadRecordIfLatest(
        (ORecordId) rid.getIdentity(), null, recordVersion, fetchPlan, ignoreCache, !ignoreCache);
  }

  /**
   * Update the record.
   *
   * @param iRecord
   * @param iForceCreate
   * @param iRecordCreatedCallback
   * @param iRecordUpdatedCallback
   */
  public ORecord saveRecord(
      final ORecord iRecord,
      final String iClusterName,
      final OPERATION_MODE iMode,
      boolean iForceCreate,
      final ORecordCallback<? extends Number> iRecordCreatedCallback,
      ORecordCallback<Integer> iRecordUpdatedCallback) {
    try {

      return database.saveAll(
          iRecord,
          iClusterName,
          iMode,
          iForceCreate,
          iRecordCreatedCallback,
          iRecordUpdatedCallback);

    } catch (Exception e) {
      // REMOVE IT FROM THE CACHE TO AVOID DIRTY RECORDS
      final ORecordId rid = (ORecordId) iRecord.getIdentity();
      if (rid.isValid()) database.getLocalCache().freeRecord(rid);

      if (e instanceof ONeedRetryException) throw (ONeedRetryException) e;

      throw OException.wrapException(
          new ODatabaseException(
              "Error during saving of record"
                  + (iRecord != null ? " with rid " + iRecord.getIdentity() : "")),
          e);
    }
  }

  @Override
  public OTransaction setIsolationLevel(final ISOLATION_LEVEL isolationLevel) {
    if (isolationLevel != ISOLATION_LEVEL.READ_COMMITTED)
      throw new IllegalArgumentException(
          "Isolation level '"
              + isolationLevel
              + "' is not supported without an active transaction");
    return super.setIsolationLevel(isolationLevel);
  }

  /** Deletes the record. */
  public void deleteRecord(final ORecord iRecord, final OPERATION_MODE iMode) {
    if (!iRecord.getIdentity().isPersistent()) return;

    try {
      database.executeDeleteRecord(iRecord, iRecord.getVersion(), true, iMode, false);
    } catch (Exception e) {
      // REMOVE IT FROM THE CACHE TO AVOID DIRTY RECORDS
      final ORecordId rid = (ORecordId) iRecord.getIdentity();
      if (rid.isValid()) database.getLocalCache().freeRecord(rid);

      if (e instanceof RuntimeException) throw (RuntimeException) e;
      throw OException.wrapException(
          new ODatabaseException(
              "Error during deletion of record"
                  + (iRecord != null ? " with rid " + iRecord.getIdentity() : "")),
          e);
    }
  }

  public Collection<ORecordOperation> getCurrentRecordEntries() {
    return null;
  }

  public Collection<ORecordOperation> getRecordOperations() {
    return null;
  }

  public List<ORecordOperation> getNewRecordEntriesByClass(
      final OClass iClass, final boolean iPolymorphic) {
    return null;
  }

  public List<ORecordOperation> getNewRecordEntriesByClusterIds(final int[] iIds) {
    return null;
  }

  public void clearRecordEntries() {}

  public int getRecordEntriesSize() {
    return 0;
  }

  public ORecord getRecord(final ORID rid) {
    return null;
  }

  public ORecordOperation getRecordEntry(final ORID rid) {
    return null;
  }

  public boolean isUsingLog() {
    return false;
  }

  @Override
  public void setCustomData(String iName, Object iValue) {}

  @Override
  public Object getCustomData(String iName) {
    return null;
  }

  public void setUsingLog(final boolean useLog) {}

  public ODocument getIndexChanges() {
    return null;
  }

  public void addIndexEntry(
      final OIndex delegate,
      final String indexName,
      final OPERATION status,
      final Object key,
      final OIdentifiable value) {
    switch (status) {
      case CLEAR:
        delegate.clear();
        break;

      case PUT:
        delegate.put(key, value);
        break;

      case REMOVE:
        delegate.remove(key, value);
        break;
    }
  }

  @Override
  public void addChangedDocument(ODocument document) {
    // do nothing
  }

  public void clearIndexEntries() {}

  public OTransactionIndexChanges getIndexChanges(final String iName) {
    return null;
  }

  public int getId() {
    return 0;
  }

  public List<String> getInvolvedIndexes() {
    return null;
  }

  public void updateIdentityAfterCommit(ORID oldRid, ORID newRid) {}

  @Override
  public int amountOfNestedTxs() {
    return 0;
  }

  @Override
  public void rollback(boolean force, int commitLevelDiff) {}

  @Override
  public OTransactionIndexChanges getIndexChangesInternal(String indexName) {
    return null;
  }

  @Override
  public void internalRollback() {}
}

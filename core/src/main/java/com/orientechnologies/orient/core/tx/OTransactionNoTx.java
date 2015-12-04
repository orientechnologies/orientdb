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
package com.orientechnologies.orient.core.tx;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.ODatabase.OPERATION_MODE;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.version.ORecordVersion;

import java.util.Collection;
import java.util.List;

/**
 * No operation transaction.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OTransactionNoTx extends OTransactionAbstract {
  public OTransactionNoTx(final ODatabaseDocumentTx iDatabase) {
    super(iDatabase);
  }

  public void begin() {
  }

  public void commit() {
  }

  @Override
  public int getEntryCount() {
    return 0;
  }

  @Override
  public boolean hasRecordCreation() {
    return false;
  }

  @Override
  public void restore() {
  }

  @Override
  public void commit(boolean force) {
  }

  public void rollback() {
  }

  @Deprecated
  public ORecord loadRecord(final ORID iRid, final ORecord iRecord, final String iFetchPlan, final boolean ignoreCache,
      final boolean loadTombstone, final OStorage.LOCKING_STRATEGY iLockingStrategy) {
    if (iRid.isNew())
      return null;

    return database.executeReadRecord((ORecordId) iRid, iRecord, null, iFetchPlan, ignoreCache, !ignoreCache, loadTombstone,
        iLockingStrategy, new ODatabaseDocumentTx.SimpleRecordReader());
  }

  @Deprecated
  public ORecord loadRecord(final ORID iRid, final ORecord iRecord, final String iFetchPlan, final boolean ignoreCache,
      final boolean iUpdateCache, final boolean loadTombstone, final OStorage.LOCKING_STRATEGY iLockingStrategy) {
    if (iRid.isNew())
      return null;

    return database.executeReadRecord((ORecordId) iRid, iRecord, null, iFetchPlan, ignoreCache, iUpdateCache, loadTombstone,
        iLockingStrategy, new ODatabaseDocumentTx.SimpleRecordReader());
  }

  public ORecord loadRecord(final ORID iRid, final ORecord iRecord, final String iFetchPlan, final boolean ignoreCache) {
    if (iRid.isNew())
      return null;

    return database.executeReadRecord((ORecordId) iRid, iRecord, null, iFetchPlan, ignoreCache, !ignoreCache, false,
        OStorage.LOCKING_STRATEGY.NONE, new ODatabaseDocumentTx.SimpleRecordReader());
  }

  @Override
  public ORecord reloadRecord(ORID rid, ORecord record, String fetchPlan, boolean ignoreCache) {
    return reloadRecord(rid, record, fetchPlan, ignoreCache, true);
  }

  @Override
  public ORecord reloadRecord(ORID rid, ORecord record, String fetchPlan, boolean ignoreCache, boolean force) {
    if (rid.isNew())
      return null;

    final ODatabaseDocumentTx.RecordReader recordReader;
    if (force) {
      recordReader = new ODatabaseDocumentTx.SimpleRecordReader();
    } else {
      recordReader = new ODatabaseDocumentTx.LatestVersionRecordReader();
    }

    final ORecord loadedRecord = database.executeReadRecord((ORecordId) rid, record, null, fetchPlan, ignoreCache, !ignoreCache,
        false, OStorage.LOCKING_STRATEGY.NONE, recordReader);

    if (force) {
      return loadedRecord;
    } else {
      if (loadedRecord == null)
        return record;

      return loadedRecord;
    }
  }

  @Override
  public ORecord loadRecordIfVersionIsNotLatest(ORID rid, ORecordVersion recordVersion, String fetchPlan, boolean ignoreCache)
      throws ORecordNotFoundException {
    if (rid.isNew())
      return null;

    return database.executeReadRecord((ORecordId) rid, null, recordVersion, fetchPlan, ignoreCache, !ignoreCache, false,
        OStorage.LOCKING_STRATEGY.NONE, new ODatabaseDocumentTx.LatestVersionRecordReader());
  }

  /**
   * Update the record.
   *
   * @param iRecord
   * @param iForceCreate
   * @param iRecordCreatedCallback
   * @param iRecordUpdatedCallback
   */
  public ORecord saveRecord(final ORecord iRecord, final String iClusterName, final OPERATION_MODE iMode, boolean iForceCreate,
      final ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    try {
      return database.executeSaveRecord(iRecord, iClusterName, iRecord.getRecordVersion(), true, iMode, iForceCreate,
          iRecordCreatedCallback, null);
    } catch (Exception e) {
      // REMOVE IT FROM THE CACHE TO AVOID DIRTY RECORDS
      final ORecordId rid = (ORecordId) iRecord.getIdentity();
      if (rid.isValid())
        database.getLocalCache().freeRecord(rid);

      if (e instanceof RuntimeException)
        throw (RuntimeException) e;
      throw new OException(e);
    }
  }

  @Override
  public OTransaction setIsolationLevel(final ISOLATION_LEVEL isolationLevel) {
    if (isolationLevel != ISOLATION_LEVEL.READ_COMMITTED)
      throw new IllegalArgumentException("Isolation level '" + isolationLevel + "' is not supported without an active transaction");
    return super.setIsolationLevel(isolationLevel);
  }

  /**
   * Deletes the record.
   */
  public void deleteRecord(final ORecord iRecord, final OPERATION_MODE iMode) {
    if (!iRecord.getIdentity().isPersistent())
      return;

    try {
      database.executeDeleteRecord(iRecord, iRecord.getRecordVersion(), true, true, iMode, false);
    } catch (Exception e) {
      // REMOVE IT FROM THE CACHE TO AVOID DIRTY RECORDS
      final ORecordId rid = (ORecordId) iRecord.getIdentity();
      if (rid.isValid())
        database.getLocalCache().freeRecord(rid);

      if (e instanceof RuntimeException)
        throw (RuntimeException) e;
      throw new OException(e);
    }
  }

  public Collection<ORecordOperation> getCurrentRecordEntries() {
    return null;
  }

  public Collection<ORecordOperation> getAllRecordEntries() {
    return null;
  }

  public List<ORecordOperation> getNewRecordEntriesByClass(final OClass iClass, final boolean iPolymorphic) {
    return null;
  }

  public List<ORecordOperation> getNewRecordEntriesByClusterIds(final int[] iIds) {
    return null;
  }

  public void clearRecordEntries() {
  }

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

  public void setUsingLog(final boolean useLog) {
  }

  public ODocument getIndexChanges() {
    return null;
  }

  public OTransactionIndexChangesPerKey getIndexEntry(final String iIndexName, final Object iKey) {
    return null;
  }

  public void addIndexEntry(final OIndex<?> delegate, final String indexName, final OPERATION status, final Object key,
      final OIdentifiable value) {
    switch (status) {
    case CLEAR:
      delegate.clear();
      break;

    case PUT:
      delegate.put(key, value);
      break;

    case REMOVE:
      assert key != null;
      delegate.remove(key, value);
      break;
    }
  }

  public void clearIndexEntries() {
  }

  public OTransactionIndexChanges getIndexChanges(final String iName) {
    return null;
  }

  public int getId() {
    return 0;
  }

  public List<String> getInvolvedIndexes() {
    return null;
  }

  public void updateIdentityAfterCommit(ORID oldRid, ORID newRid) {
  }

  @Override
  public int amountOfNestedTxs() {
    return 0;
  }

  @Override
  public void rollback(boolean force, int commitLevelDiff) {
  }
}

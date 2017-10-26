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
import com.orientechnologies.orient.core.db.document.LatestVersionRecordReader;
import com.orientechnologies.orient.core.db.document.RecordReader;
import com.orientechnologies.orient.core.db.document.SimpleRecordReader;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODirtyManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * No operation transaction.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OTransactionNoTx extends OTransactionAbstract {
  public OTransactionNoTx(final ODatabaseDocumentInternal iDatabase) {
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
  public void commit(boolean force) {
  }

  public void rollback() {
  }

  @Deprecated
  public ORecord loadRecord(final ORID iRid, final ORecord iRecord, final String iFetchPlan, final boolean ignoreCache,
      final boolean loadTombstone, final OStorage.LOCKING_STRATEGY iLockingStrategy) {
    if (iRid.isNew())
      return null;

    return database
        .executeReadRecord((ORecordId) iRid, iRecord, -1, iFetchPlan, ignoreCache, !ignoreCache, loadTombstone, iLockingStrategy,
            new SimpleRecordReader(database.isPrefetchRecords()));
  }

  @Deprecated
  public ORecord loadRecord(final ORID iRid, final ORecord iRecord, final String iFetchPlan, final boolean ignoreCache,
      final boolean iUpdateCache, final boolean loadTombstone, final OStorage.LOCKING_STRATEGY iLockingStrategy) {
    if (iRid.isNew())
      return null;

    return database
        .executeReadRecord((ORecordId) iRid, iRecord, -1, iFetchPlan, ignoreCache, iUpdateCache, loadTombstone, iLockingStrategy,
            new SimpleRecordReader(database.isPrefetchRecords()));
  }

  public ORecord loadRecord(final ORID iRid, final ORecord iRecord, final String iFetchPlan, final boolean ignoreCache) {
    if (iRid.isNew())
      return null;

    return database.executeReadRecord((ORecordId) iRid, iRecord, -1, iFetchPlan, ignoreCache, !ignoreCache, false,
        OStorage.LOCKING_STRATEGY.NONE, new SimpleRecordReader(database.isPrefetchRecords()));
  }

  @Override
  public ORecord reloadRecord(ORID rid, ORecord record, String fetchPlan, boolean ignoreCache) {
    return reloadRecord(rid, record, fetchPlan, ignoreCache, true);
  }

  @Override
  public ORecord reloadRecord(ORID rid, ORecord record, String fetchPlan, boolean ignoreCache, boolean force) {
    if (rid.isNew())
      return null;

    final RecordReader recordReader;
    if (force) {
      recordReader = new SimpleRecordReader(database.isPrefetchRecords());
    } else {
      recordReader = new LatestVersionRecordReader();
    }

    final ORecord loadedRecord = database
        .executeReadRecord((ORecordId) rid, record, -1, fetchPlan, ignoreCache, !ignoreCache, false, OStorage.LOCKING_STRATEGY.NONE,
            recordReader);

    if (force) {
      return loadedRecord;
    } else {
      if (loadedRecord == null)
        return record;

      return loadedRecord;
    }
  }

  @Override
  public ORecord loadRecordIfVersionIsNotLatest(ORID rid, int recordVersion, String fetchPlan, boolean ignoreCache)
      throws ORecordNotFoundException {
    if (rid.isNew())
      return null;

    return database.executeReadRecord((ORecordId) rid, null, recordVersion, fetchPlan, ignoreCache, !ignoreCache, false,
        OStorage.LOCKING_STRATEGY.NONE, new LatestVersionRecordReader());
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
      final ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<Integer> iRecordUpdatedCallback) {
    try {

      ORecord toRet = null;
      ODirtyManager dirtyManager = ORecordInternal.getDirtyManager(iRecord);
      Set<ORecord> newRecord = dirtyManager.getNewRecords();
      Set<ORecord> updatedRecord = dirtyManager.getUpdateRecords();
      dirtyManager.clearForSave();
      if (newRecord != null) {
        for (ORecord rec : newRecord) {
          if (rec.getIdentity().isNew() && rec instanceof ODocument) {
            ORecord ret = saveNew((ODocument) rec, dirtyManager, iClusterName, iRecord, iMode, iForceCreate, iRecordCreatedCallback,
                iRecordUpdatedCallback);
            if (ret != null)
              toRet = ret;
          }
        }
      }
      if (updatedRecord != null) {
        for (ORecord rec : updatedRecord) {
          if (rec == iRecord) {
            toRet = database.executeSaveRecord(rec, iClusterName, rec.getVersion(), iMode, iForceCreate, iRecordCreatedCallback,
                iRecordUpdatedCallback);
          } else
            database.executeSaveRecord(rec, getClusterName(rec), rec.getVersion(), OPERATION_MODE.SYNCHRONOUS, false, null, null);
        }
      }

      if (toRet != null)
        return toRet;
      else
        return database.executeSaveRecord(iRecord, iClusterName, iRecord.getVersion(), iMode, iForceCreate, iRecordCreatedCallback,
            iRecordUpdatedCallback);
    } catch (Exception e) {
      // REMOVE IT FROM THE CACHE TO AVOID DIRTY RECORDS
      final ORecordId rid = (ORecordId) iRecord.getIdentity();
      if (rid.isValid())
        database.getLocalCache().freeRecord(rid);

      if (e instanceof ONeedRetryException)
        throw (ONeedRetryException) e;

      throw OException.wrapException(
          new ODatabaseException("Error during saving of record" + (iRecord != null ? " with rid " + iRecord.getIdentity() : "")),
          e);
    }
  }

  public ORecord saveNew(ODocument document, ODirtyManager manager, String iClusterName, ORecord original,
      final OPERATION_MODE iMode, boolean iForceCreate, final ORecordCallback<? extends Number> iRecordCreatedCallback,
      ORecordCallback<Integer> iRecordUpdatedCallback) {
    ORecord toRet = null;
    LinkedList<ODocument> path = new LinkedList<ODocument>();
    ORecord next = document;
    do {
      if (next instanceof ODocument) {
        ORecord nextToInspect = null;
        List<OIdentifiable> toSave = manager.getPointed(next);
        if (toSave != null) {
          for (OIdentifiable oIdentifiable : toSave) {
            if (oIdentifiable.getIdentity().isNew()) {
              if (oIdentifiable instanceof ORecord)
                nextToInspect = (ORecord) oIdentifiable;
              else
                nextToInspect = oIdentifiable.getRecord();
              break;
            }
          }
        }
        if (nextToInspect != null) {
          if (path.contains(nextToInspect)) {
            if (nextToInspect == original)
              database.executeSaveEmptyRecord(nextToInspect, iClusterName);
            else
              database.executeSaveEmptyRecord(nextToInspect, getClusterName(nextToInspect));
          } else {
            path.push((ODocument) next);
            next = nextToInspect;
          }
        } else {
          if (next == original)
            toRet = database.executeSaveRecord(next, iClusterName, next.getVersion(), iMode, iForceCreate, iRecordCreatedCallback,
                iRecordUpdatedCallback);
          else
            database
                .executeSaveRecord(next, getClusterName(next), next.getVersion(), OPERATION_MODE.SYNCHRONOUS, false, null, null);
          next = path.pollFirst();
        }

      } else {
        database.executeSaveRecord(next, null, next.getVersion(), iMode, false, null, null);
        next = path.pollFirst();
      }
    } while (next != null);
    return toRet;
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
      database.executeDeleteRecord(iRecord, iRecord.getVersion(), true, iMode, false);
    } catch (Exception e) {
      // REMOVE IT FROM THE CACHE TO AVOID DIRTY RECORDS
      final ORecordId rid = (ORecordId) iRecord.getIdentity();
      if (rid.isValid())
        database.getLocalCache().freeRecord(rid);

      if (e instanceof RuntimeException)
        throw (RuntimeException) e;
      throw OException.wrapException(
          new ODatabaseException("Error during deletion of record" + (iRecord != null ? " with rid " + iRecord.getIdentity() : "")),
          e);
    }
  }

  public Collection<ORecordOperation> getCurrentRecordEntries() {
    return null;
  }

  public Collection<ORecordOperation> getRecordOperations() {
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

  @Override
  public void setCustomData(String iName, Object iValue) {

  }

  @Override
  public Object getCustomData(String iName) {
    return null;
  }

  public void setUsingLog(final boolean useLog) {
  }

  public ODocument getIndexChanges() {
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
      delegate.remove(key, value);
      break;
    }
  }

  @Override
  public void addChangedDocument(ODocument document) {
    // do nothing
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

  @Override
  public OTransactionIndexChanges getIndexChangesInternal(String indexName) {
    return null;
  }

  @Override
  public void internalRollback() {

  }
}

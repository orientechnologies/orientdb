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
package com.orientechnologies.orient.core.tx;

import java.util.Collection;
import java.util.List;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.ODatabaseComplex.OPERATION_MODE;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.version.ORecordVersion;

/**
 * No operation transaction.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OTransactionNoTx extends OTransactionAbstract {
  public OTransactionNoTx(final ODatabaseRecordTx iDatabase) {
    super(iDatabase);
  }

  public void begin() {
  }

  public void commit() {
  }

  public void rollback() {
  }

  public void close() {
  }

  public ORecordInternal<?> loadRecord(final ORID iRid, final ORecordInternal<?> iRecord, final String iFetchPlan,
      boolean ignonreCache, boolean loadTombstone) {
    if (iRid.isNew())
      return null;

    return database.executeReadRecord((ORecordId) iRid, iRecord, iFetchPlan, ignonreCache, loadTombstone);
  }

  /**
   * Update the record.
   * 
   * @param iForceCreate
   * @param iRecordCreatedCallback
   * @param iRecordUpdatedCallback
   */
  public void saveRecord(final ORecordInternal<?> iRecord, final String iClusterName, final OPERATION_MODE iMode,
      boolean iForceCreate, final ORecordCallback<? extends Number> iRecordCreatedCallback,
      ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    try {
      database.executeSaveRecord(iRecord, iClusterName, iRecord.getRecordVersion(), iRecord.getRecordType(), true, iMode,
          iForceCreate, iRecordCreatedCallback, null);
    } catch (Exception e) {
      // REMOVE IT FROM THE CACHE TO AVOID DIRTY RECORDS
      final ORecordId rid = (ORecordId) iRecord.getIdentity();
      if (rid.isValid())
        database.getLevel1Cache().freeRecord(rid);

      if (e instanceof RuntimeException)
        throw (RuntimeException) e;
      throw new OException(e);
    }
  }

  @Override
  public boolean updateReplica(ORecordInternal<?> iRecord) {
    try {
      return database.executeUpdateReplica(iRecord);
    } catch (Exception e) {
      // REMOVE IT FROM THE CACHE TO AVOID DIRTY RECORDS
      final ORecordId rid = (ORecordId) iRecord.getIdentity();
      database.getLevel1Cache().freeRecord(rid);

      if (e instanceof RuntimeException)
        throw (RuntimeException) e;
      throw new OException(e);
    }
  }

  /**
   * Deletes the record.
   */
  public void deleteRecord(final ORecordInternal<?> iRecord, final OPERATION_MODE iMode) {
    if (!iRecord.getIdentity().isPersistent())
      return;

    try {
      database.executeDeleteRecord(iRecord, iRecord.getRecordVersion(), true, true, iMode, false);
    } catch (Exception e) {
      // REMOVE IT FROM THE CACHE TO AVOID DIRTY RECORDS
      final ORecordId rid = (ORecordId) iRecord.getIdentity();
      if (rid.isValid())
        database.getLevel1Cache().freeRecord(rid);

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

  public List<ORecordOperation> getRecordEntriesByClass(String iClassName) {
    return null;
  }

  public List<ORecordOperation> getNewRecordEntriesByClusterIds(int[] iIds) {
    return null;
  }

  public void clearRecordEntries() {
  }

  public int getRecordEntriesSize() {
    return 0;
  }

  public ORecordInternal<?> getRecord(final ORID rid) {
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

  public void addIndexEntry(final OIndex<?> delegate, final String iIndexName, final OPERATION iStatus, final Object iKey,
      final OIdentifiable iValue) {
    switch (iStatus) {
    case CLEAR:
      delegate.clear();
      break;

    case PUT:
      delegate.put(iKey, iValue);
      break;

    case REMOVE:
      if (iKey == null)
        delegate.remove(iValue);
      else
        delegate.remove(iKey, iValue);
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
}

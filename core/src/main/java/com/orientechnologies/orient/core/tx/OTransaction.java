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

import com.orientechnologies.orient.core.db.ODatabase.OPERATION_MODE;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.version.ORecordVersion;

import java.util.HashMap;
import java.util.List;

public interface OTransaction {
  public enum TXTYPE {
    NOTX, OPTIMISTIC, PESSIMISTIC
  }

  public enum TXSTATUS {
    INVALID, BEGUN, COMMITTING, ROLLBACKING, COMPLETED, ROLLED_BACK
  }

  public enum ISOLATION_LEVEL {
    READ_COMMITTED, REPEATABLE_READ
  }

  public void begin();

  public void commit();

  public void commit(boolean force);

  public void rollback();

  /**
   * Returns the current isolation level.
   */
  public ISOLATION_LEVEL getIsolationLevel();

  /**
   * Changes the isolation level. Default is READ_COMMITTED. When REPEATABLE_READ is set, any record read from the storage is cached
   * in memory to guarantee the repeatable reads. This affects the used RAM and speed (because JVM Garbage Collector job).
   *
   * @param iIsolationLevel
   *          Isolation level to set
   * @return Current object to allow call in chain
   */
  public OTransaction setIsolationLevel(ISOLATION_LEVEL iIsolationLevel);

  public void rollback(boolean force, int commitLevelDiff);

  public ODatabaseDocument getDatabase();

  public void clearRecordEntries();

  @Deprecated
  public ORecord loadRecord(ORID iRid, ORecord iRecord, String iFetchPlan, boolean ignoreCache, boolean loadTombstone,
      final OStorage.LOCKING_STRATEGY iLockingStrategy);

  public ORecord loadRecord(ORID iRid, ORecord iRecord, String iFetchPlan, boolean ignoreCache);

  public ORecord saveRecord(ORecord iRecord, String iClusterName, OPERATION_MODE iMode, boolean iForceCreate,
      ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<ORecordVersion> iRecordUpdatedCallback);

  public void deleteRecord(ORecord iRecord, OPERATION_MODE iMode);

  public int getId();

  public TXSTATUS getStatus();

  public Iterable<? extends ORecordOperation> getCurrentRecordEntries();

  public Iterable<? extends ORecordOperation> getAllRecordEntries();

  public List<ORecordOperation> getNewRecordEntriesByClass(OClass iClass, boolean iPolymorphic);

  public List<ORecordOperation> getNewRecordEntriesByClusterIds(int[] iIds);

  public ORecord getRecord(ORID iRid);

  public ORecordOperation getRecordEntry(ORID rid);

  public List<String> getInvolvedIndexes();

  public ODocument getIndexChanges();

  public void addIndexEntry(OIndex<?> delegate, final String iIndexName, final OTransactionIndexChanges.OPERATION iStatus,
      final Object iKey, final OIdentifiable iValue);

  public void clearIndexEntries();

  public OTransactionIndexChanges getIndexChanges(String iName);

  /**
   * Tells if the transaction is active.
   *
   * @return
   */
  public boolean isActive();

  public boolean isUsingLog();

  /**
   * If you set this flag to false, you are unable to
   * <ol>
   * <li>Rollback data changes in case of exception</li>
   * <li>Restore data in case of server crash</li>
   * </ol>
   *
   * So you practically unable to work in multithreaded environment and keep data consistent.
   */
  public void setUsingLog(boolean useLog);

  public void close();

  /**
   * When commit in transaction is performed all new records will change their identity, but index values will contain stale links,
   * to fix them given method will be called for each entry. This update local transaction maps too.
   *
   * @param oldRid
   *          Record identity before commit.
   * @param newRid
   *          Record identity after commit.
   */
  public void updateIdentityAfterCommit(final ORID oldRid, final ORID newRid);

  public int amountOfNestedTxs();

  public boolean isLockedRecord(OIdentifiable iRecord);

  OStorage.LOCKING_STRATEGY lockingStrategy(OIdentifiable iRecord);

  public OTransaction lockRecord(OIdentifiable iRecord, OStorage.LOCKING_STRATEGY iLockingStrategy);

  public OTransaction unlockRecord(OIdentifiable iRecord);

  public HashMap<ORID, OStorage.LOCKING_STRATEGY> getLockedRecords();

  public int getEntryCount();

  public boolean hasRecordCreation();
}
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
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
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
  ORecord loadRecord(ORID iRid, ORecord iRecord, String iFetchPlan, boolean ignoreCache, boolean loadTombstone,
      final OStorage.LOCKING_STRATEGY iLockingStrategy);

  @Deprecated
  ORecord loadRecord(ORID iRid, ORecord iRecord, String iFetchPlan, boolean ignoreCache, boolean iUpdateCache, boolean loadTombstone,
                      final OStorage.LOCKING_STRATEGY iLockingStrategy);

  ORecord loadRecord(ORID iRid, ORecord iRecord, String iFetchPlan, boolean ignoreCache);

  ORecord reloadRecord(ORID iRid, ORecord iRecord, String iFetchPlan, boolean ignoreCache);

  ORecord reloadRecord(ORID iRid, ORecord iRecord, String iFetchPlan, boolean ignoreCache, boolean force);

  ORecord loadRecordIfVersionIsNotLatest(ORID rid, ORecordVersion recordVersion, String fetchPlan, boolean ignoreCache)
      throws ORecordNotFoundException;

  ORecord saveRecord(ORecord iRecord, String iClusterName, OPERATION_MODE iMode, boolean iForceCreate,
      ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<ORecordVersion> iRecordUpdatedCallback);

  void deleteRecord(ORecord iRecord, OPERATION_MODE iMode);

  int getId();

  TXSTATUS getStatus();

  Iterable<? extends ORecordOperation> getCurrentRecordEntries();

  Iterable<? extends ORecordOperation> getAllRecordEntries();

  List<ORecordOperation> getNewRecordEntriesByClass(OClass iClass, boolean iPolymorphic);

  List<ORecordOperation> getNewRecordEntriesByClusterIds(int[] iIds);

  ORecord getRecord(ORID iRid);

  ORecordOperation getRecordEntry(ORID rid);

  List<String> getInvolvedIndexes();

  ODocument getIndexChanges();

  void addIndexEntry(OIndex<?> delegate, final String iIndexName, final OTransactionIndexChanges.OPERATION iStatus,
      final Object iKey, final OIdentifiable iValue);

  void clearIndexEntries();

  OTransactionIndexChanges getIndexChanges(String iName);

  /**
   * Tells if the transaction is active.
   * 
   * @return
   */
  boolean isActive();

  boolean isUsingLog();

  /**
   * If you set this flag to false, you are unable to
   * <ol>
   * <li>Rollback data changes in case of exception</li>
   * <li>Restore data in case of server crash</li>
   * </ol>
   * 
   * So you practically unable to work in multithreaded environment and keep data consistent.
   */
  void setUsingLog(boolean useLog);

  void close();

  /**
   * When commit in transaction is performed all new records will change their identity, but index values will contain stale links,
   * to fix them given method will be called for each entry. This update local transaction maps too.
   * 
   * @param oldRid
   *          Record identity before commit.
   * @param newRid
   *          Record identity after commit.
   */
  void updateIdentityAfterCommit(final ORID oldRid, final ORID newRid);

  int amountOfNestedTxs();

  boolean isLockedRecord(OIdentifiable iRecord);

  OStorage.LOCKING_STRATEGY lockingStrategy(OIdentifiable iRecord);

  OTransaction lockRecord(OIdentifiable iRecord, OStorage.LOCKING_STRATEGY iLockingStrategy);

  OTransaction unlockRecord(OIdentifiable iRecord);

  HashMap<ORID, OStorage.LOCKING_STRATEGY> getLockedRecords();

  int getEntryCount();

  boolean hasRecordCreation();

  /**
   * Restores a partially committed transaction to the tial
   */
  void restore();
}

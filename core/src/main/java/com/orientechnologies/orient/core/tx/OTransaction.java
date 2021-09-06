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

import com.orientechnologies.orient.core.db.ODatabase;
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
import java.util.List;

public interface OTransaction {
  enum TXTYPE {
    NOTX,
    OPTIMISTIC,
    PESSIMISTIC
  }

  enum TXSTATUS {
    INVALID,
    BEGUN,
    COMMITTING,
    ROLLBACKING,
    COMPLETED,
    ROLLED_BACK
  }

  enum ISOLATION_LEVEL {
    READ_COMMITTED,
    REPEATABLE_READ
  }

  void begin();

  void commit();

  void commit(boolean force);

  void rollback();

  /** Returns the current isolation level. */
  ISOLATION_LEVEL getIsolationLevel();

  /**
   * Changes the isolation level. Default is READ_COMMITTED. When REPEATABLE_READ is set, any record
   * read from the storage is cached in memory to guarantee the repeatable reads. This affects the
   * used RAM and speed (because JVM Garbage Collector job).
   *
   * @param iIsolationLevel Isolation level to set
   * @return Current object to allow call in chain
   */
  OTransaction setIsolationLevel(ISOLATION_LEVEL iIsolationLevel);

  void rollback(boolean force, int commitLevelDiff);

  ODatabaseDocument getDatabase();

  @Deprecated
  void clearRecordEntries();

  @Deprecated
  ORecord loadRecord(
      ORID iRid,
      ORecord iRecord,
      String iFetchPlan,
      boolean ignoreCache,
      boolean loadTombstone,
      final OStorage.LOCKING_STRATEGY iLockingStrategy);

  @Deprecated
  ORecord loadRecord(
      ORID iRid,
      ORecord iRecord,
      String iFetchPlan,
      boolean ignoreCache,
      boolean iUpdateCache,
      boolean loadTombstone,
      final OStorage.LOCKING_STRATEGY iLockingStrategy);

  ORecord loadRecord(ORID iRid, ORecord iRecord, String iFetchPlan, boolean ignoreCache);

  ORecord reloadRecord(ORID iRid, ORecord iRecord, String iFetchPlan, boolean ignoreCache);

  ORecord reloadRecord(
      ORID iRid, ORecord iRecord, String iFetchPlan, boolean ignoreCache, boolean force);

  ORecord loadRecordIfVersionIsNotLatest(
      ORID rid, int recordVersion, String fetchPlan, boolean ignoreCache)
      throws ORecordNotFoundException;

  TXSTATUS getStatus();

  @Deprecated
  Iterable<? extends ORecordOperation> getCurrentRecordEntries();

  Iterable<? extends ORecordOperation> getRecordOperations();

  List<ORecordOperation> getNewRecordEntriesByClass(OClass iClass, boolean iPolymorphic);

  List<ORecordOperation> getNewRecordEntriesByClusterIds(int[] iIds);

  ORecordOperation getRecordEntry(ORID rid);

  List<String> getInvolvedIndexes();

  ODocument getIndexChanges();

  @Deprecated
  void clearIndexEntries();

  boolean isUsingLog();

  /**
   * If you set this flag to false, you are unable to
   *
   * <ol>
   *   <li>Rollback data changes in case of exception
   *   <li>Restore data in case of server crash
   * </ol>
   *
   * <p>So you practically unable to work in multithreaded environment and keep data consistent.
   *
   * @deprecated This option has no effect
   */
  @Deprecated
  void setUsingLog(boolean useLog);

  void close();

  /**
   * When commit in transaction is performed all new records will change their identity, but index
   * values will contain stale links, to fix them given method will be called for each entry. This
   * update local transaction maps too.
   *
   * @param oldRid Record identity before commit.
   * @param newRid Record identity after commit.
   */
  void updateIdentityAfterCommit(final ORID oldRid, final ORID newRid);

  int amountOfNestedTxs();

  boolean isLockedRecord(OIdentifiable iRecord);

  @Deprecated
  OStorage.LOCKING_STRATEGY lockingStrategy(OIdentifiable iRecord);

  @Deprecated
  OTransaction lockRecord(OIdentifiable iRecord, OStorage.LOCKING_STRATEGY iLockingStrategy);

  @Deprecated
  OTransaction unlockRecord(OIdentifiable iRecord);

  int getEntryCount();

  /** @return {@code true} if this transaction is active, {@code false} otherwise. */
  boolean isActive();

  /**
   * Saves the given record in this transaction.
   *
   * @param record the record to save.
   * @param clusterName record's cluster name.
   * @param operationMode the operation mode.
   * @param forceCreate the force creation flag, {@code true} to force the creation of the record,
   *     {@code false} to allow updates.
   * @param createdCallback the callback to invoke when the record save operation triggered the
   *     creation of the record.
   * @param updatedCallback the callback to invoke when the record save operation triggered the
   *     update of the record.
   * @return the record saved.
   */
  ORecord saveRecord(
      ORecord record,
      String clusterName,
      ODatabase.OPERATION_MODE operationMode,
      boolean forceCreate,
      ORecordCallback<? extends Number> createdCallback,
      ORecordCallback<Integer> updatedCallback);

  /**
   * Deletes the given record in this transaction.
   *
   * @param record the record to delete.
   * @param mode the operation mode.
   */
  void deleteRecord(ORecord record, ODatabase.OPERATION_MODE mode);

  /**
   * Resolves a record with the given RID in the context of this transaction.
   *
   * @param rid the record RID.
   * @return the resolved record, or {@code null} if no record is found, or {@link
   *     OTransactionAbstract#DELETED_RECORD} if the record was deleted in this transaction.
   */
  ORecord getRecord(ORID rid);

  /**
   * Adds the transactional index entry in this transaction.
   *
   * @param index the index.
   * @param indexName the index name.
   * @param operation the index operation to register.
   * @param key the index key.
   * @param value the index key value.
   */
  void addIndexEntry(
      OIndex index,
      String indexName,
      OTransactionIndexChanges.OPERATION operation,
      Object key,
      OIdentifiable value);

  /**
   * Adds the given document to a set of changed documents known to this transaction.
   *
   * @param document the document to add.
   */
  void addChangedDocument(ODocument document);

  /**
   * Obtains the index changes done in the context of this transaction.
   *
   * @param indexName the index name.
   * @return the index changes in question or {@code null} if index is not found.
   */
  OTransactionIndexChanges getIndexChanges(String indexName);

  /**
   * Does the same thing as {@link #getIndexChanges(String)}, but handles remote storages in a
   * special way.
   *
   * @param indexName the index name.
   * @return the index changes in question or {@code null} if index is not found or storage is
   *     remote.
   */
  OTransactionIndexChanges getIndexChangesInternal(String indexName);

  /**
   * Obtains the custom value by its name stored in the context of this transaction.
   *
   * @param name the value name.
   * @return the obtained value or {@code null} if no value found.
   */
  Object getCustomData(String name);

  /**
   * Sets the custom value by its name stored in the context of this transaction.
   *
   * @param name the value name.
   * @param value the value to store.
   */
  void setCustomData(String name, Object value);

  /** @return this transaction ID as seen by the client of this transaction. */
  default int getClientTransactionId() {
    return getId();
  }

  int getId();
}

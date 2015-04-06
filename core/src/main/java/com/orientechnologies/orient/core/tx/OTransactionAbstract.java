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

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

public abstract class OTransactionAbstract implements OTransaction {
  protected final ODatabaseDocumentTx                database;
  protected TXSTATUS                                 status         = TXSTATUS.INVALID;
  protected ISOLATION_LEVEL                          isolationLevel = ISOLATION_LEVEL.READ_COMMITTED;
  protected HashMap<ORID, OStorage.LOCKING_STRATEGY> locks          = new HashMap<ORID, OStorage.LOCKING_STRATEGY>();

  protected OTransactionAbstract(final ODatabaseDocumentTx iDatabase) {
    database = iDatabase;
  }

  public static void updateCacheFromEntries(final OTransaction tx, final Iterable<? extends ORecordOperation> entries,
      final boolean updateStrategy) {
    final OLocalRecordCache dbCache = tx.getDatabase().getLocalCache();

    for (ORecordOperation txEntry : entries) {
      if (!updateStrategy)
        // ALWAYS REMOVE THE RECORD FROM CACHE
        dbCache.deleteRecord(txEntry.getRecord().getIdentity());
      else if (txEntry.type == ORecordOperation.DELETED)
        // DELETION
        dbCache.deleteRecord(txEntry.getRecord().getIdentity());
      else if (txEntry.type == ORecordOperation.UPDATED || txEntry.type == ORecordOperation.CREATED)
        // UDPATE OR CREATE
        dbCache.updateRecord(txEntry.getRecord());
    }
  }

  @Override
  public ISOLATION_LEVEL getIsolationLevel() {
    return isolationLevel;
  }

  @Override
  public OTransaction setIsolationLevel(final ISOLATION_LEVEL isolationLevel) {
    if (isolationLevel == ISOLATION_LEVEL.REPEATABLE_READ && getDatabase().getStorage() instanceof OStorageProxy)
      throw new IllegalArgumentException("Remote storage does not support isolation level '" + isolationLevel + "'");

    this.isolationLevel = isolationLevel;
    return this;
  }

  public boolean isActive() {
    return status != TXSTATUS.INVALID && status != TXSTATUS.COMPLETED && status != TXSTATUS.ROLLED_BACK;
  }

  public TXSTATUS getStatus() {
    return status;
  }

  public ODatabaseDocumentTx getDatabase() {
    return database;
  }

  /**
   * Closes the transaction and releases all the acquired locks.
   */
  @Override
  public void close() {
    for (Map.Entry<ORID, OStorage.LOCKING_STRATEGY> lock : locks.entrySet()) {
      try {
        if (lock.getValue().equals(OStorage.LOCKING_STRATEGY.KEEP_EXCLUSIVE_LOCK))
          ((OAbstractPaginatedStorage) getDatabase().getStorage()).releaseWriteLock(lock.getKey());
        else if (lock.getValue().equals(OStorage.LOCKING_STRATEGY.KEEP_SHARED_LOCK))
          ((OAbstractPaginatedStorage) getDatabase().getStorage()).releaseReadLock(lock.getKey());
      } catch (Exception e) {
        OLogManager.instance().debug(this, "Error on releasing lock against record " + lock.getKey(), e);
      }
    }
    locks.clear();
  }

  @Override
  public OTransaction lockRecord(final OIdentifiable iRecord, final OStorage.LOCKING_STRATEGY iLockingStrategy) {
    final OStorage stg = database.getStorage();
    if (!(stg.getUnderlying() instanceof OAbstractPaginatedStorage))
      throw new OLockException("Cannot lock record across remote connections");

    final ORID rid = iRecord.getIdentity();
    // if (locks.containsKey(rid))
    // throw new IllegalStateException("Record " + rid + " is already locked");

    if (iLockingStrategy == OStorage.LOCKING_STRATEGY.KEEP_EXCLUSIVE_LOCK)
      ((OAbstractPaginatedStorage) stg.getUnderlying()).acquireWriteLock(rid);
    else
      ((OAbstractPaginatedStorage) stg.getUnderlying()).acquireReadLock(rid);

    locks.put(rid, iLockingStrategy);
    return this;
  }

  @Override
  public boolean isLockedRecord(final OIdentifiable iRecord) {
    final ORID rid = iRecord.getIdentity();
    OStorage.LOCKING_STRATEGY iLockingStrategy = locks.get(rid);
    if (iLockingStrategy == null) 
       return false;
    else 
       return true;
  }

  @Override
  public OTransaction unlockRecord(final OIdentifiable iRecord) {
    final OStorage stg = database.getStorage();
    if (!(stg.getUnderlying() instanceof OAbstractPaginatedStorage))
      throw new OLockException("Cannot lock record across remote connections");

    final ORID rid = iRecord.getIdentity();

    final OStorage.LOCKING_STRATEGY lock = locks.remove(rid);

    if (lock == null)
      throw new OLockException("Cannot unlock a never acquired lock");
    else if (lock == OStorage.LOCKING_STRATEGY.KEEP_EXCLUSIVE_LOCK)
      ((OAbstractPaginatedStorage) stg.getUnderlying()).releaseWriteLock(rid);
    else
      ((OAbstractPaginatedStorage) stg.getUnderlying()).releaseReadLock(rid);

    return this;
  }

  @Override
  public HashMap<ORID, OStorage.LOCKING_STRATEGY> getLockedRecords() {
    return locks;
  }
}

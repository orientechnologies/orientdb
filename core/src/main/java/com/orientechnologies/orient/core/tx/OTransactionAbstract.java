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

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class OTransactionAbstract implements OTransaction {
  protected ODatabaseDocumentInternal database;
  protected TXSTATUS status = TXSTATUS.INVALID;
  protected ISOLATION_LEVEL isolationLevel = ISOLATION_LEVEL.READ_COMMITTED;
  protected Map<ORID, LockedRecordMetadata> locks = new HashMap<ORID, LockedRecordMetadata>();
  /**
   * Indicates the record deleted in a transaction.
   *
   * @see #getRecord(ORID)
   */
  public static final ORecord DELETED_RECORD = new ORecordBytes();

  public static final class LockedRecordMetadata {
    private final OStorage.LOCKING_STRATEGY strategy;
    private int locksCount;

    public LockedRecordMetadata(OStorage.LOCKING_STRATEGY strategy) {
      this.strategy = strategy;
    }
  }

  protected OTransactionAbstract(final ODatabaseDocumentInternal iDatabase) {
    database = iDatabase;
  }

  public static void updateCacheFromEntries(
      final ODatabaseDocumentInternal database,
      final Iterable<? extends ORecordOperation> entries,
      final boolean updateStrategy) {
    final OLocalRecordCache dbCache = database.getLocalCache();

    for (ORecordOperation txEntry : entries) {
      if (!updateStrategy) {
        // ALWAYS REMOVE THE RECORD FROM CACHE
        dbCache.deleteRecord(txEntry.getRecord().getIdentity());
      } else if (txEntry.type == ORecordOperation.DELETED) {
        // DELETION
        dbCache.deleteRecord(txEntry.getRecord().getIdentity());
      } else if (txEntry.type == ORecordOperation.UPDATED
          || txEntry.type == ORecordOperation.CREATED) {
        // UPDATE OR CREATE
        dbCache.updateRecord(txEntry.getRecord());
      }
      if (txEntry.getRecord() instanceof ODocument) {
        ODocumentInternal.clearTransactionTrackData((ODocument) txEntry.getRecord());
      }
    }
  }

  @Override
  public ISOLATION_LEVEL getIsolationLevel() {
    return isolationLevel;
  }

  @Override
  public OTransaction setIsolationLevel(final ISOLATION_LEVEL isolationLevel) {
    if (isolationLevel == ISOLATION_LEVEL.REPEATABLE_READ && getDatabase().isRemote())
      throw new IllegalArgumentException(
          "Remote storage does not support isolation level '" + isolationLevel + "'");

    this.isolationLevel = isolationLevel;
    return this;
  }

  public boolean isActive() {
    return status != TXSTATUS.INVALID
        && status != TXSTATUS.COMPLETED
        && status != TXSTATUS.ROLLED_BACK;
  }

  public TXSTATUS getStatus() {
    return status;
  }

  public ODatabaseDocumentInternal getDatabase() {
    return database;
  }

  /** Closes the transaction and releases all the acquired locks. */
  @Override
  public void close() {
    for (Map.Entry<ORID, LockedRecordMetadata> lock : locks.entrySet()) {
      try {
        final LockedRecordMetadata lockedRecordMetadata = lock.getValue();

        if (lockedRecordMetadata.strategy.equals(OStorage.LOCKING_STRATEGY.EXCLUSIVE_LOCK)) {
          ((OAbstractPaginatedStorage) getDatabase().getStorage()).releaseWriteLock(lock.getKey());
        } else if (lockedRecordMetadata.strategy.equals(OStorage.LOCKING_STRATEGY.SHARED_LOCK)) {
          ((OAbstractPaginatedStorage) getDatabase().getStorage()).releaseReadLock(lock.getKey());
        }
      } catch (Exception e) {
        OLogManager.instance()
            .debug(this, "Error on releasing lock against record " + lock.getKey(), e);
      }
    }
    locks.clear();
  }

  @Override
  public OTransaction lockRecord(
      final OIdentifiable iRecord, final OStorage.LOCKING_STRATEGY lockingStrategy) {
    database.internalLockRecord(iRecord, lockingStrategy);
    return this;
  }

  @Override
  public boolean isLockedRecord(final OIdentifiable iRecord) {
    final ORID rid = iRecord.getIdentity();
    final LockedRecordMetadata lockedRecordMetadata = locks.get(rid);

    return lockedRecordMetadata != null && lockedRecordMetadata.locksCount != 0;
  }

  @Override
  public OStorage.LOCKING_STRATEGY lockingStrategy(OIdentifiable record) {
    final ORID rid = record.getIdentity();
    final LockedRecordMetadata lockedRecordMetadata = locks.get(rid);

    if (lockedRecordMetadata == null || lockedRecordMetadata.locksCount == 0) return null;

    return lockedRecordMetadata.strategy;
  }

  @Override
  public OTransaction unlockRecord(final OIdentifiable iRecord) {
    database.internalUnlockRecord(iRecord);
    return this;
  }

  public abstract void internalRollback();

  public void trackLockedRecord(ORID rid, OStorage.LOCKING_STRATEGY lockingStrategy) {
    OTransactionAbstract.LockedRecordMetadata lockedRecordMetadata = locks.get(rid);
    boolean addItem = false;

    if (lockedRecordMetadata == null) {
      lockedRecordMetadata = new OTransactionAbstract.LockedRecordMetadata(lockingStrategy);
      addItem = true;
    } else if (lockedRecordMetadata.strategy != lockingStrategy) {
      assert lockedRecordMetadata.locksCount == 0;
      lockedRecordMetadata = new OTransactionAbstract.LockedRecordMetadata(lockingStrategy);
      addItem = true;
    }
    lockedRecordMetadata.locksCount++;
    if (addItem) {
      locks.put(rid, lockedRecordMetadata);
    }
  }

  public OStorage.LOCKING_STRATEGY trackUnlockRecord(ORID rid) {
    final LockedRecordMetadata lockedRecordMetadata = locks.get(rid);
    if (lockedRecordMetadata != null && lockedRecordMetadata.locksCount > 0) {
      lockedRecordMetadata.locksCount--;
      if (lockedRecordMetadata.locksCount == 0) {
        locks.remove(rid);
        return lockedRecordMetadata.strategy;
      }
    } else {
      throw new OLockException("Cannot unlock a never acquired lock");
    }
    return null;
  }

  public Map<ORID, LockedRecordMetadata> getInternalLocks() {
    return locks;
  }

  protected void setLocks(Map<ORID, LockedRecordMetadata> locks) {
    this.locks = locks;
  }

  public Set<ORID> getLockedRecords() {
    return locks.keySet();
  }

  public void setDatabase(ODatabaseDocumentInternal database) {
    this.database = database;
  }
}

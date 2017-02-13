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

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

import java.util.HashMap;
import java.util.Map;

public abstract class OTransactionAbstract implements OTransaction {
  protected final ODatabaseDocumentTx           database;
  protected TXSTATUS                            status         = TXSTATUS.INVALID;
  protected ISOLATION_LEVEL                     isolationLevel = ISOLATION_LEVEL.READ_COMMITTED;
  protected HashMap<ORID, LockedRecordMetadata> locks          = new HashMap<ORID, LockedRecordMetadata>();

  private static final class LockedRecordMetadata {
    private final OStorage.LOCKING_STRATEGY strategy;
    private int                             locksCount;

    public LockedRecordMetadata(OStorage.LOCKING_STRATEGY strategy) {
      this.strategy = strategy;
    }
  }

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
    for (Map.Entry<ORID, LockedRecordMetadata> lock : locks.entrySet()) {
      try {
        final LockedRecordMetadata lockedRecordMetadata = lock.getValue();

        if (lockedRecordMetadata.strategy.equals(OStorage.LOCKING_STRATEGY.EXCLUSIVE_LOCK)) {
          for (int i = 0; i < lockedRecordMetadata.locksCount; i++) {
            ((OAbstractPaginatedStorage) getDatabase().getStorage().getUnderlying()).releaseWriteLock(lock.getKey());
          }
        } else if (lockedRecordMetadata.strategy.equals(OStorage.LOCKING_STRATEGY.SHARED_LOCK)) {
          for (int i = 0; i < lockedRecordMetadata.locksCount; i++) {
            ((OAbstractPaginatedStorage) getDatabase().getStorage().getUnderlying()).releaseReadLock(lock.getKey());
          }
        }
      } catch (Exception e) {
        OLogManager.instance().debug(this, "Error on releasing lock against record " + lock.getKey(), e);
      }
    }
    locks.clear();
  }

  @Override
  public OTransaction lockRecord(final OIdentifiable iRecord, final OStorage.LOCKING_STRATEGY lockingStrategy) {
    final OStorage stg = database.getStorage();
    if (!(stg.getUnderlying() instanceof OAbstractPaginatedStorage))
      throw new OLockException("Cannot lock record across remote connections");

    final ORID rid = new ORecordId(iRecord.getIdentity());

    LockedRecordMetadata lockedRecordMetadata = locks.get(rid);
    boolean addItem = false;

    if (lockedRecordMetadata == null) {
      lockedRecordMetadata = new LockedRecordMetadata(lockingStrategy);
      addItem = true;
    } else if (lockedRecordMetadata.strategy != lockingStrategy) {
      assert lockedRecordMetadata.locksCount == 0;
      lockedRecordMetadata = new LockedRecordMetadata(lockingStrategy);
      addItem = true;
    }

    if (lockingStrategy == OStorage.LOCKING_STRATEGY.EXCLUSIVE_LOCK)
      ((OAbstractPaginatedStorage) stg.getUnderlying()).acquireWriteLock(rid);
    else if (lockingStrategy == OStorage.LOCKING_STRATEGY.SHARED_LOCK)
      ((OAbstractPaginatedStorage) stg.getUnderlying()).acquireReadLock(rid);
    else
      throw new IllegalStateException("Unsupported locking strategy " + lockingStrategy);

    lockedRecordMetadata.locksCount++;

    if (addItem) {
      locks.put(rid, lockedRecordMetadata);
    }

    return this;
  }

  @Override
  public boolean isLockedRecord(final OIdentifiable iRecord) {
    final ORID rid = iRecord.getIdentity();
    final LockedRecordMetadata lockedRecordMetadata = locks.get(rid);

    if (lockedRecordMetadata == null || lockedRecordMetadata.locksCount == 0)
      return false;
    else
      return true;
  }

  @Override
  public OStorage.LOCKING_STRATEGY lockingStrategy(OIdentifiable record) {
    final ORID rid = record.getIdentity();
    final LockedRecordMetadata lockedRecordMetadata = locks.get(rid);

    if (lockedRecordMetadata == null || lockedRecordMetadata.locksCount == 0)
      return null;

    return lockedRecordMetadata.strategy;
  }

  @Override
  public OTransaction unlockRecord(final OIdentifiable iRecord) {
    final OStorage stg = database.getStorage();
    if (!(stg.getUnderlying() instanceof OAbstractPaginatedStorage))
      throw new OLockException("Cannot lock record across remote connections");

    final ORID rid = iRecord.getIdentity();

    final LockedRecordMetadata lockedRecordMetadata = locks.get(rid);

    if (lockedRecordMetadata == null || lockedRecordMetadata.locksCount == 0)
      throw new OLockException("Cannot unlock a never acquired lock");
    else if (lockedRecordMetadata.strategy == OStorage.LOCKING_STRATEGY.EXCLUSIVE_LOCK)
      ((OAbstractPaginatedStorage) stg.getUnderlying()).releaseWriteLock(rid);
    else if (lockedRecordMetadata.strategy == OStorage.LOCKING_STRATEGY.SHARED_LOCK)
      ((OAbstractPaginatedStorage) stg.getUnderlying()).releaseReadLock(rid);
    else
      throw new IllegalStateException("Unsupported locking strategy " + lockedRecordMetadata.strategy);

    lockedRecordMetadata.locksCount--;

    if (lockedRecordMetadata.locksCount == 0)
      locks.remove(rid);

    return this;
  }

  @Override
  public HashMap<ORID, OStorage.LOCKING_STRATEGY> getLockedRecords() {
    final HashMap<ORID, OStorage.LOCKING_STRATEGY> lockedRecords = new HashMap<ORID, OStorage.LOCKING_STRATEGY>();

    for (Map.Entry<ORID, LockedRecordMetadata> entry : locks.entrySet()) {
      if (entry.getValue().locksCount > 0)
        lockedRecords.put(entry.getKey(), entry.getValue().strategy);
    }

    return lockedRecords;
  }

  public String getClusterName(final ORecord record) {
    if (ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().isRemote())
      // DON'T ASSIGN CLUSTER WITH REMOTE: SERVER KNOWS THE RIGHT CLUSTER BASED ON LOCALITY
      return null;

    int clusterId = record.getIdentity().getClusterId();
    if (clusterId == ORID.CLUSTER_ID_INVALID) {
      // COMPUTE THE CLUSTER ID
      OClass schemaClass = null;
      if (record instanceof ODocument)
        schemaClass = ODocumentInternal.getImmutableSchemaClass((ODocument) record);
      if (schemaClass != null) {
        // FIND THE RIGHT CLUSTER AS CONFIGURED IN CLASS
        if (schemaClass.isAbstract())
          throw new OSchemaException("Document belongs to abstract class '" + schemaClass.getName() + "' and cannot be saved");
        clusterId = schemaClass.getClusterForNewInstance((ODocument) record);
        return database.getClusterNameById(clusterId);
      } else {
        return database.getClusterNameById(database.getStorage().getDefaultClusterId());
      }

    } else {
      return database.getClusterNameById(clusterId);
    }
  }

}

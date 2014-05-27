/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.tx;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseComplex.OPERATION_MODE;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal;
import com.orientechnologies.orient.core.db.OScenarioThreadLocal.RUN_MODE;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.engine.local.OEngineLocal;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexAbstract;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAwareAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.core.version.ORecordVersion;

public class OTransactionOptimistic extends OTransactionRealAbstract {
  private static final boolean useSBTree = OGlobalConfiguration.INDEX_USE_SBTREE_BY_DEFAULT.getValueAsBoolean();
  private static AtomicInteger txSerial  = new AtomicInteger();
  private boolean              usingLog;
  private int                  txStartCounter;

  public OTransactionOptimistic(final ODatabaseRecordTx iDatabase) {
    super(iDatabase, txSerial.incrementAndGet());
    usingLog = OGlobalConfiguration.TX_USE_LOG.getValueAsBoolean();
  }

  public void begin() {
    if (txStartCounter == 0)
      status = TXSTATUS.BEGUN;

    txStartCounter++;

    if (txStartCounter > 1)
      OLogManager.instance().debug(this, "Transaction was already started and will be reused.");
  }

  public void commit() {
    commit(false);
  }

  /**
   * The transaction is reentrant. If {@code begin()} has been called several times, the actual commit happens only after the same
   * amount of {@code commit()} calls
   * 
   * @param force
   *          commit transaction even
   */
  @Override
  public void commit(final boolean force) {
    checkTransaction();

    if (force)
      txStartCounter = 0;
    else
      txStartCounter--;

    if (txStartCounter == 0) {
      doCommit();
    } else if (txStartCounter > 0)
      OLogManager.instance().debug(this, "Nested transaction was closed but transaction itself was not committed.");
    else
      throw new OTransactionException("Transaction was committed more times than it is started.");
  }

  private void doCommit() {
    if (status == TXSTATUS.ROLLED_BACK || status == TXSTATUS.ROLLBACKING)
      throw new ORollbackException("Given transaction was rolled back and can not be used.");

    status = TXSTATUS.COMMITTING;

    if (OScenarioThreadLocal.INSTANCE.get() != RUN_MODE.RUNNING_DISTRIBUTED && !(database.getStorage() instanceof OStorageEmbedded))
      database.getStorage().commit(this, null);
    else {
      List<OIndexAbstract<?>> lockedIndexes = acquireIndexLocks();
      try {
        final Map<String, OIndex> indexes = new HashMap<String, OIndex>();
        for (OIndex index : database.getMetadata().getIndexManager().getIndexes())
          indexes.put(index.getName(), index);

        final Runnable callback = new CommitIndexesCallback(indexes);

        final String storageType = database.getStorage().getType();

        if (storageType.equals(OEngineLocal.NAME) || storageType.equals(OEngineLocalPaginated.NAME))
          database.getStorage().commit(OTransactionOptimistic.this, callback);
        else {
          database.getStorage().callInLock(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
              database.getStorage().commit(OTransactionOptimistic.this, null);
              callback.run();
              return null;
            }
          }, true);
        }

      } finally {
        releaseIndexLocks(lockedIndexes);
      }
    }

    close();

    status = TXSTATUS.COMPLETED;
  }

  private List<OIndexAbstract<?>> acquireIndexLocks() {
    List<OIndexAbstract<?>> lockedIndexes = null;
    final List<String> involvedIndexes = getInvolvedIndexes();

    if (involvedIndexes != null)
      Collections.sort(involvedIndexes);

    try {
      // LOCK INVOLVED INDEXES
      if (involvedIndexes != null)
        for (String indexName : involvedIndexes) {
          final OIndexAbstract<?> index = (OIndexAbstract<?>) database.getMetadata().getIndexManager().getIndexInternal(indexName);
          if (lockedIndexes == null)
            lockedIndexes = new ArrayList<OIndexAbstract<?>>();

          index.acquireModificationLock();
          lockedIndexes.add(index);
        }

      if (!useSBTree) {
        // SEARCH FOR INDEX BASED ON DOCUMENT TOUCHED
        final Collection<? extends OIndex<?>> indexes = database.getMetadata().getIndexManager().getIndexes();
        List<? extends OIndex<?>> indexesToLock = null;
        if (indexes != null) {
          indexesToLock = new ArrayList<OIndex<?>>(indexes);
          Collections.sort(indexesToLock, new Comparator<OIndex<?>>() {
            public int compare(final OIndex<?> indexOne, final OIndex<?> indexTwo) {
              return indexOne.getName().compareTo(indexTwo.getName());
            }
          });
        }

        if (indexesToLock != null && !indexesToLock.isEmpty()) {
          if (lockedIndexes == null)
            lockedIndexes = new ArrayList<OIndexAbstract<?>>();

          for (OIndex<?> index : indexesToLock) {
            for (Entry<ORID, ORecordOperation> entry : recordEntries.entrySet()) {
              final ORecord<?> record = entry.getValue().record.getRecord();
              if (record instanceof ODocument) {
                ODocument doc = (ODocument) record;
                if (!lockedIndexes.contains(index.getInternal()) && doc.getSchemaClass() != null && index.getDefinition() != null
                    && doc.getSchemaClass().isSubClassOf(index.getDefinition().getClassName())) {
                  index.getInternal().acquireModificationLock();
                  lockedIndexes.add((OIndexAbstract<?>) index.getInternal());
                }
              }
            }
          }

          for (OIndexAbstract<?> index : lockedIndexes)
            index.acquireExclusiveLock();
        }
      }
      return lockedIndexes;
    } catch (RuntimeException e) {
      releaseIndexLocks(lockedIndexes);
      throw e;
    }
  }

  private void releaseIndexLocks(List<OIndexAbstract<?>> lockedIndexes) {
    if (lockedIndexes != null) {
      if (!useSBTree) {
        for (OIndexAbstract<?> index : lockedIndexes)
          index.releaseExclusiveLock();
      }

      for (OIndexAbstract<?> index : lockedIndexes)
        index.releaseModificationLock();

    }
  }

  private class CommitIndexesCallback implements Runnable {
    private final Map<String, OIndex> indexes;

    private CommitIndexesCallback(Map<String, OIndex> indexes) {
      this.indexes = indexes;
    }

    @Override
    public void run() {
      final ODocument indexEntries = getIndexChanges();
      if (indexEntries != null) {
        final Map<String, OIndexInternal<?>> indexesToCommit = new HashMap<String, OIndexInternal<?>>();

        for (Entry<String, Object> indexEntry : indexEntries) {
          final OIndexInternal<?> index = indexes.get(indexEntry.getKey()).getInternal();
          indexesToCommit.put(index.getName(), index.getInternal());
        }

        for (OIndexInternal<?> indexInternal : indexesToCommit.values())
          indexInternal.preCommit();

        for (Entry<String, Object> indexEntry : indexEntries) {
          final OIndexInternal<?> index = indexesToCommit.get(indexEntry.getKey()).getInternal();

          if (index == null) {
            OLogManager.instance().error(this, "Index with name " + indexEntry.getKey() + " was not found.");
            throw new OIndexException("Index with name " + indexEntry.getKey() + " was not found.");
          } else
            index.addTxOperation((ODocument) indexEntry.getValue());
        }

        try {
          for (OIndexInternal<?> indexInternal : indexesToCommit.values())
            indexInternal.commit();
        } finally {
          for (OIndexInternal<?> indexInternal : indexesToCommit.values())
            indexInternal.postCommit();
        }
      }
    }
  }

  @Override
  public int amountOfNestedTxs() {
    return txStartCounter;
  }

  public void rollback() {
    rollback(false, -1);
  }

  @Override
  public void rollback(boolean force, int commitLevelDiff) {
    checkTransaction();

    txStartCounter += commitLevelDiff;
    status = TXSTATUS.ROLLBACKING;

    if (!force && txStartCounter > 0) {
      OLogManager.instance().debug(this, "Nested transaction was closed but transaction itself was scheduled for rollback.");
      return;
    }

    if (txStartCounter < 0)
      throw new OTransactionException("Transaction was rolled back more times than it was started.");

    database.getStorage().callInLock(new Callable<Void>() {

      public Void call() throws Exception {

        database.getStorage().rollback(OTransactionOptimistic.this);
        return null;
      }
    }, true);

    // CLEAR THE CACHE MOVING GOOD RECORDS TO LEVEL-2 CACHE
    database.getLevel1Cache().clear();

    // REMOVE ALL THE ENTRIES AND INVALIDATE THE DOCUMENTS TO AVOID TO BE RE-USED DIRTY AT USER-LEVEL. IN THIS WAY RE-LOADING MUST
    // EXECUTED
    for (ORecordOperation v : recordEntries.values())
      v.getRecord().unload();

    for (ORecordOperation v : allEntries.values())
      v.getRecord().unload();

    close();

    status = TXSTATUS.ROLLED_BACK;
  }

  public ORecordInternal<?> loadRecord(final ORID iRid, final ORecordInternal<?> iRecord, final String iFetchPlan,
      final boolean ignoreCache, final boolean loadTombstone, final OStorage.LOCKING_STRATEGY iLockingStrategy) {
    checkTransaction();

    final ORecordInternal<?> txRecord = getRecord(iRid);
    if (txRecord == OTransactionRealAbstract.DELETED_RECORD)
      // DELETED IN TX
      return null;

    if (txRecord != null) {
      if (iRecord != null && txRecord != iRecord)
        OLogManager.instance().warn(
            this,
            "Found record in transaction with the same RID %s but different instance. "
                + "Probably the record has been loaded from another transaction and reused on the current one: reload it "
                + "from current transaction before to update or delete it", iRecord.getIdentity());
      return txRecord;
    }

    if (iRid.isTemporary())
      return null;

    // DELEGATE TO THE STORAGE, NO TOMBSTONES SUPPORT IN TX MODE
    final ORecordInternal<?> record = database.executeReadRecord((ORecordId) iRid, iRecord, iFetchPlan, ignoreCache, false,
        iLockingStrategy);

    if (record != null)
      addRecord(record, ORecordOperation.LOADED, null);

    return record;
  }

  public void deleteRecord(final ORecordInternal<?> iRecord, final OPERATION_MODE iMode) {
    if (!iRecord.getIdentity().isValid())
      return;

    addRecord(iRecord, ORecordOperation.DELETED, null);
  }

  public void saveRecord(final ORecordInternal<?> iRecord, final String iClusterName, final OPERATION_MODE iMode,
      boolean iForceCreate, final ORecordCallback<? extends Number> iRecordCreatedCallback,
      ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    if (iRecord == null)
      return;
    final byte operation = iForceCreate ? ORecordOperation.CREATED : iRecord.getIdentity().isValid() ? ORecordOperation.UPDATED
        : ORecordOperation.CREATED;
    addRecord(iRecord, operation, iClusterName);
  }

  @Override
  public boolean updateReplica(ORecordInternal<?> iRecord) {
    throw new UnsupportedOperationException("updateReplica()");
  }

  @Override
  public String toString() {
    return "OTransactionOptimistic [id=" + id + ", status=" + status + ", recEntries=" + recordEntries.size() + ", idxEntries="
        + indexEntries.size() + ']';
  }

  public boolean isUsingLog() {
    return usingLog;
  }

  public void setUsingLog(final boolean useLog) {
    this.usingLog = useLog;
  }

  protected void addRecord(final ORecordInternal<?> iRecord, final byte iStatus, final String iClusterName) {
    checkTransaction();

    switch (iStatus) {
    case ORecordOperation.CREATED:
      database.checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_CREATE, iClusterName);
      database.callbackHooks(TYPE.BEFORE_CREATE, iRecord);
      break;
    case ORecordOperation.LOADED:
      /**
       * Read hooks already invoked in {@link com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract#executeReadRecord}
       * .
       */
      break;
    case ORecordOperation.UPDATED:
      database.checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_UPDATE, iClusterName);
      database.callbackHooks(TYPE.BEFORE_UPDATE, iRecord);
      break;
    case ORecordOperation.DELETED:
      database.checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_DELETE, iClusterName);
      database.callbackHooks(TYPE.BEFORE_DELETE, iRecord);
      break;
    }

    try {
      if (iRecord.getIdentity().isTemporary())
        temp2persistent.put(iRecord.getIdentity().copy(), iRecord);

      if ((status == OTransaction.TXSTATUS.COMMITTING) && database.getStorage() instanceof OStorageEmbedded) {

        // I'M COMMITTING: BYPASS LOCAL BUFFER
        switch (iStatus) {
        case ORecordOperation.CREATED:
        case ORecordOperation.UPDATED:
          final ORID oldRid = iRecord.getIdentity().copy();
          database.executeSaveRecord(iRecord, iClusterName, iRecord.getRecordVersion(), false, OPERATION_MODE.SYNCHRONOUS, false,
              null, null);
          updateIdentityAfterCommit(oldRid, iRecord.getIdentity());
          break;
        case ORecordOperation.DELETED:
          database.executeDeleteRecord(iRecord, iRecord.getRecordVersion(), false, false, OPERATION_MODE.SYNCHRONOUS, false);
          break;
        }

        final ORecordOperation txRecord = getRecordEntry(iRecord.getIdentity());

        if (txRecord == null) {
          // NOT IN TX, SAVE IT ANYWAY
          allEntries.put(iRecord.getIdentity(), new ORecordOperation(iRecord, iStatus));
        } else if (txRecord.record != iRecord) {
          // UPDATE LOCAL RECORDS TO AVOID MISMATCH OF VERSION/CONTENT
          final String clusterName = getDatabase().getClusterNameById(iRecord.getIdentity().getClusterId());
          if (!clusterName.equals(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME)
              && !clusterName.equals(OMetadataDefault.CLUSTER_INDEX_NAME))
            OLogManager
                .instance()
                .warn(
                    this,
                    "Found record in transaction with the same RID %s but different instance. Probably the record has been loaded from another transaction and reused on the current one: reload it from current transaction before to update or delete it",
                    iRecord.getIdentity());

          txRecord.record = iRecord;
          txRecord.type = iStatus;
        }

      } else {
        final ORecordId rid = (ORecordId) iRecord.getIdentity();

        if (!rid.isValid()) {
          iRecord.onBeforeIdentityChanged(rid);

          // ASSIGN A UNIQUE SERIAL TEMPORARY ID
          if (rid.clusterId == ORID.CLUSTER_ID_INVALID)
            rid.clusterId = iClusterName != null ? database.getClusterIdByName(iClusterName) : database.getDefaultClusterId();

          if (database.getStorageVersions().classesAreDetectedByClusterId() && iRecord instanceof ORecordSchemaAwareAbstract) {
            final ORecordSchemaAwareAbstract recordSchemaAware = (ORecordSchemaAwareAbstract) iRecord;
            final OClass recordClass = recordSchemaAware.getSchemaClass();
            final OClass clusterIdClass = database.getMetadata().getSchema().getClassByClusterId(rid.clusterId);
            if (recordClass == null && clusterIdClass != null || clusterIdClass == null && recordClass != null
                || (recordClass != null && !recordClass.equals(clusterIdClass)))
              throw new OSchemaException("Record saved into cluster " + iClusterName + " should be saved with class "
                  + clusterIdClass + " but saved with class " + recordClass);
          }

          rid.clusterPosition = OClusterPositionFactory.INSTANCE.valueOf(newObjectCounter--);

          iRecord.onAfterIdentityChanged(iRecord);
        } else
          // REMOVE FROM THE DB'S CACHE
          database.getLevel1Cache().freeRecord(rid);

        ORecordOperation txEntry = getRecordEntry(rid);

        if (txEntry == null) {
          if (!(rid.isTemporary() && iStatus != ORecordOperation.CREATED)) {
            // NEW ENTRY: JUST REGISTER IT
            txEntry = new ORecordOperation(iRecord, iStatus);
            recordEntries.put(rid, txEntry);
          }
        } else {
          // UPDATE PREVIOUS STATUS
          txEntry.record = iRecord;

          switch (txEntry.type) {
          case ORecordOperation.LOADED:
            switch (iStatus) {
            case ORecordOperation.UPDATED:
              txEntry.type = ORecordOperation.UPDATED;
              break;
            case ORecordOperation.DELETED:
              txEntry.type = ORecordOperation.DELETED;
              break;
            }
            break;
          case ORecordOperation.UPDATED:
            switch (iStatus) {
            case ORecordOperation.DELETED:
              txEntry.type = ORecordOperation.DELETED;
              break;
            }
            break;
          case ORecordOperation.DELETED:
            break;
          case ORecordOperation.CREATED:
            switch (iStatus) {
            case ORecordOperation.DELETED:
              recordEntries.remove(rid);
              break;
            }
            break;
          }
        }
      }

      switch (iStatus) {
      case ORecordOperation.CREATED:
        database.callbackHooks(TYPE.AFTER_CREATE, iRecord);
        break;
      case ORecordOperation.LOADED:
        /**
         * Read hooks already invoked in
         * {@link com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract#executeReadRecord}.
         */
        break;
      case ORecordOperation.UPDATED:
        database.callbackHooks(TYPE.AFTER_UPDATE, iRecord);
        break;
      case ORecordOperation.DELETED:
        database.callbackHooks(TYPE.AFTER_DELETE, iRecord);
        break;
      }
    } catch (Throwable t) {
      switch (iStatus) {
      case ORecordOperation.CREATED:
        database.callbackHooks(TYPE.CREATE_FAILED, iRecord);
        break;
      case ORecordOperation.UPDATED:
        database.callbackHooks(TYPE.UPDATE_FAILED, iRecord);
        break;
      case ORecordOperation.DELETED:
        database.callbackHooks(TYPE.DELETE_FAILED, iRecord);
        break;
      }

      if (t instanceof RuntimeException)
        throw (RuntimeException) t;
      else
        throw new ODatabaseException("Error on saving record " + iRecord.getIdentity(), t);
    }
  }
}

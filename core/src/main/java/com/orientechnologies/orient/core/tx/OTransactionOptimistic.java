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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseComplex.OPERATION_MODE;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexMVRBTreeAbstract;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.version.ORecordVersion;

public class OTransactionOptimistic extends OTransactionRealAbstract {
  private boolean              usingLog;
  private static AtomicInteger txSerial = new AtomicInteger();

  public OTransactionOptimistic(final ODatabaseRecordTx iDatabase) {
    super(iDatabase, txSerial.incrementAndGet());
    usingLog = OGlobalConfiguration.TX_USE_LOG.getValueAsBoolean();
  }

  public void begin() {
    status = TXSTATUS.BEGUN;
  }

  public void commit() {
    checkTransaction();
    status = TXSTATUS.COMMITTING;

    if (database.getStorage() instanceof OStorageProxy)
      database.getStorage().commit(this);
    else {
      final List<String> involvedIndexes = getInvolvedIndexes();

      if (involvedIndexes != null)
        Collections.sort(involvedIndexes);

      // LOCK INVOLVED INDEXES
      List<OIndexMVRBTreeAbstract<?>> lockedIndexes = null;
      try {
        if (involvedIndexes != null)
          for (String indexName : involvedIndexes) {
            final OIndexMVRBTreeAbstract<?> index = (OIndexMVRBTreeAbstract<?>) database.getMetadata().getIndexManager()
                .getIndexInternal(indexName);
            if (lockedIndexes == null)
              lockedIndexes = new ArrayList<OIndexMVRBTreeAbstract<?>>();

            index.acquireModificationLock();
            lockedIndexes.add(index);
          }

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

        if (indexesToLock != null && !indexesToLock.isEmpty())
          if (lockedIndexes == null)
            lockedIndexes = new ArrayList<OIndexMVRBTreeAbstract<?>>();

        for (OIndex<?> index : indexesToLock) {
          for (Entry<ORID, ORecordOperation> entry : recordEntries.entrySet()) {
            final ORecord<?> record = entry.getValue().record.getRecord();
            if (record instanceof ODocument) {
              ODocument doc = (ODocument) record;
              if (!lockedIndexes.contains(index.getInternal()) && doc.getSchemaClass() != null && index.getDefinition() != null
                  && doc.getSchemaClass().isSubClassOf(index.getDefinition().getClassName())) {
                index.getInternal().acquireModificationLock();
                lockedIndexes.add((OIndexMVRBTreeAbstract<?>) index.getInternal());
              }
            }
          }
        }

        for (OIndexMVRBTreeAbstract<?> index : lockedIndexes)
          index.acquireExclusiveLock();

        database.getStorage().callInLock(new Callable<Void>() {

          public Void call() throws Exception {

            database.getStorage().commit(OTransactionOptimistic.this);

            // COMMIT INDEX CHANGES
            final ODocument indexEntries = getIndexChanges();
            if (indexEntries != null) {
              for (Entry<String, Object> indexEntry : indexEntries) {
                final OIndex<?> index = database.getMetadata().getIndexManager().getIndexInternal(indexEntry.getKey());
                index.commit((ODocument) indexEntry.getValue());
              }
            }
            return null;
          }

        }, true);
      } finally {
        // RELEASE INDEX LOCKS IF ANY
        if (lockedIndexes != null) {
          for (OIndexMVRBTreeAbstract<?> index : lockedIndexes)
            index.releaseExclusiveLock();

          for (OIndexMVRBTreeAbstract<?> index : lockedIndexes)
            index.releaseModificationLock();

        }
      }
    }
  }

  public void rollback() {
    checkTransaction();

    status = TXSTATUS.ROLLBACKING;

    // CLEAR THE CACHE MOVING GOOD RECORDS TO LEVEL-2 CACHE
    database.getLevel1Cache().clear();

    // REMOVE ALL THE ENTRIES AND INVALIDATE THE DOCUMENTS TO AVOID TO BE RE-USED DIRTY AT USER-LEVEL. IN THIS WAY RE-LOADING MUST
    // EXECUTED
    for (ORecordOperation v : recordEntries.values())
      v.getRecord().unload();

    for (ORecordOperation v : allEntries.values())
      v.getRecord().unload();

    indexEntries.clear();
  }

  public ORecordInternal<?> loadRecord(final ORID iRid, final ORecordInternal<?> iRecord, final String iFetchPlan) {
    checkTransaction();

    final ORecordInternal<?> txRecord = getRecord(iRid);
    if (txRecord == OTransactionRealAbstract.DELETED_RECORD)
      // DELETED IN TX
      return null;

    if (txRecord != null)
      return txRecord;

    // DELEGATE TO THE STORAGE
    return database.executeReadRecord((ORecordId) iRid, iRecord, iFetchPlan, false);
  }

  public void deleteRecord(final ORecordInternal<?> iRecord, final OPERATION_MODE iMode) {
    if (!iRecord.getIdentity().isValid())
      return;

    addRecord(iRecord, ORecordOperation.DELETED, null);
  }

  public void saveRecord(final ORecordInternal<?> iRecord, final String iClusterName, final OPERATION_MODE iMode,
                         boolean iForceCreate, final ORecordCallback<? extends Number> iRecordCreatedCallback, ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    if (iRecord == null)
      return;
    addRecord(iRecord, iRecord.getIdentity().isValid() ? ORecordOperation.UPDATED : ORecordOperation.CREATED, iClusterName);
  }

  protected void addRecord(final ORecordInternal<?> iRecord, final byte iStatus, final String iClusterName) {
    checkTransaction();

    switch (iStatus) {
    case ORecordOperation.CREATED:
      database.callbackHooks(TYPE.BEFORE_CREATE, iRecord);
      break;
    case ORecordOperation.LOADED:
      database.callbackHooks(TYPE.BEFORE_READ, iRecord);
      break;
    case ORecordOperation.UPDATED:
      database.callbackHooks(TYPE.BEFORE_UPDATE, iRecord);
      break;
    case ORecordOperation.DELETED:
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
          database.executeSaveRecord(iRecord, iClusterName, iRecord.getRecordVersion(), iRecord.getRecordType(), false,
              OPERATION_MODE.SYNCHRONOUS, false, null, null);
          break;
        case ORecordOperation.DELETED:
          database.executeDeleteRecord(iRecord, iRecord.getRecordVersion(), false, false, OPERATION_MODE.SYNCHRONOUS);
          break;
        }
      } else {
        final ORecordId rid = (ORecordId) iRecord.getIdentity();

        if (!rid.isValid()) {
          iRecord.onBeforeIdentityChanged(rid);

          // ASSIGN A UNIQUE SERIAL TEMPORARY ID
          if (rid.clusterId == ORID.CLUSTER_ID_INVALID)
            rid.clusterId = iClusterName != null ? database.getClusterIdByName(iClusterName) : database.getDefaultClusterId();
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
        database.callbackHooks(TYPE.AFTER_READ, iRecord);
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
    }
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
}

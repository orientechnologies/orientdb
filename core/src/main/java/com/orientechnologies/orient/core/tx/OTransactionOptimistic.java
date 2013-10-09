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

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseComplex.OPERATION_MODE;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.engine.local.OEngineLocal;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexAbstract;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.core.version.ORecordVersion;

public class OTransactionOptimistic extends OTransactionRealAbstract {
  private static final boolean          useSBTree                 = OGlobalConfiguration.INDEX_USE_SBTREE_BY_DEFAULT
                                                                      .getValueAsBoolean();

  private boolean                       usingLog;
  private static AtomicInteger          txSerial                  = new AtomicInteger();
  protected static ThreadLocal<Boolean> addNewRecordsRecursivelly = new ThreadLocal<Boolean>() {
                                                                    @Override
                                                                    protected Boolean initialValue() {
                                                                      return true;
                                                                    }
                                                                  };

  private int                           autoRetries               = OGlobalConfiguration.TX_AUTO_RETRY.getValueAsInteger();

  public OTransactionOptimistic(final ODatabaseRecordTx iDatabase) {
    super(iDatabase, txSerial.incrementAndGet());
    usingLog = OGlobalConfiguration.TX_USE_LOG.getValueAsBoolean();
  }

  public void begin() {
    status = TXSTATUS.BEGUN;
  }

  public Map<ORID, ORID> commit() {
    checkTransaction();
    status = TXSTATUS.COMMITTING;

    if (database.getStorage() instanceof OStorageProxy)
      database.getStorage().commit(this, null);
    else {
      final List<String> involvedIndexes = getInvolvedIndexes();

      if (involvedIndexes != null)
        Collections.sort(involvedIndexes);

      for (int retry = 1; retry <= autoRetries; ++retry) {
        try {

          // LOCK INVOLVED INDEXES
          List<OIndexAbstract<?>> lockedIndexes = null;
          try {
            if (involvedIndexes != null)
              for (String indexName : involvedIndexes) {
                final OIndexAbstract<?> index = (OIndexAbstract<?>) database.getMetadata().getIndexManager()
                    .getIndexInternal(indexName);
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
                      if (!lockedIndexes.contains(index.getInternal()) && doc.getSchemaClass() != null
                          && index.getDefinition() != null
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

            final Map<String, OIndex> indexes = new HashMap<String, OIndex>();
            for (OIndex index : database.getMetadata().getIndexManager().getIndexes()) {
              indexes.put(index.getName().toLowerCase(), index);
            }

            final Runnable callback = new Runnable() {
              @Override
              public void run() {
                final ODocument indexEntries = getIndexChanges();
                if (indexEntries != null) {
                  for (Entry<String, Object> indexEntry : indexEntries) {
                    final OIndex<?> index = indexes.get(indexEntry.getKey().toLowerCase());

                    if (index == null) {
                      OLogManager.instance().error(this, "Index with name " + indexEntry.getKey() + " was not found.");
                      throw new OIndexException("Index with name " + indexEntry.getKey() + " was not found.");
                    } else
                      index.commit((ODocument) indexEntry.getValue());
                  }
                }
              }
            };

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
            // RELEASE INDEX LOCKS IF ANY
            if (lockedIndexes != null) {
              if (!useSBTree) {
                for (OIndexAbstract<?> index : lockedIndexes)
                  index.releaseExclusiveLock();
              }

              for (OIndexAbstract<?> index : lockedIndexes)
                index.releaseModificationLock();
            }
          }
        } catch (OTimeoutException e) {
          if (autoRetries == 0) {
            OLogManager.instance().debug(this, "Caught timeout exception during commit, but no automatic retry has been set", e);
            throw e;
          } else if (retry == autoRetries) {
            OLogManager.instance().debug(this, "Caught timeout exception during %d/%d. Retry limit is exceeded.", retry,
                autoRetries);
            throw e;
          } else {
            OLogManager.instance().debug(this, "Caught timeout exception during commit retrying %d/%d...", retry, autoRetries);
          }
        }
      }
    }

    final Map<ORID, ORID> createdRecords = new HashMap<ORID, ORID>();

    for (Map.Entry<ORID, ORecord<?>> tempEntry : temp2persistent.entrySet()) {
      createdRecords.put(tempEntry.getKey(), tempEntry.getValue().getIdentity().copy());
    }

    return createdRecords;
  }

  public void rollback() {
    checkTransaction();

    status = TXSTATUS.ROLLBACKING;

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

    indexEntries.clear();
  }

  public ORecordInternal<?> loadRecord(final ORID iRid, final ORecordInternal<?> iRecord, final String iFetchPlan,
      boolean ignoreCache, boolean loadTombstone) {
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
    final ORecordInternal<?> record = database.executeReadRecord((ORecordId) iRid, iRecord, iFetchPlan, ignoreCache, false);

    if (record != null) {
      addNewRecordsRecursivelly.set(false);
      try {
        addRecord(record, ORecordOperation.LOADED, null);
      } finally {
        addNewRecordsRecursivelly.set(true);
      }
    }

    return record;
  }

  public void deleteRecord(final ORecordInternal<?> iRecord, final OPERATION_MODE iMode) {
    if (!iRecord.getIdentity().isValid())
      return;

    addNewRecordsRecursivelly.set(false);
    try {
      addRecord(iRecord, ORecordOperation.DELETED, null);
    } finally {
      addNewRecordsRecursivelly.set(true);
    }
  }

  public void saveRecord(final ORecordInternal<?> iRecord, final String iClusterName, final OPERATION_MODE iMode,
      boolean iForceCreate, final ORecordCallback<? extends Number> iRecordCreatedCallback,
      ORecordCallback<ORecordVersion> iRecordUpdatedCallback) {
    if (iRecord == null)
      return;
    addRecord(iRecord, iRecord.getIdentity().isValid() ? ORecordOperation.UPDATED : ORecordOperation.CREATED, iClusterName);
  }

  protected void addRecord(final ORecordInternal<?> record, final byte status, String clusterName) {
    checkTransaction();

    if (addNewRecordsRecursivelly.get()) {
      Set<ORecordInternal<?>> newRecords = Collections.newSetFromMap(new IdentityHashMap<ORecordInternal<?>, Boolean>());
      Set<Object> processedValues = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());

      extractAllNewNotEmbeddedRecords(record, newRecords, processedValues);

      newRecords.remove(record);

      addNewRecordsRecursivelly.set(false);
      try {
        ODatabaseDocumentTx databaseDocumentTx = database.getDatabaseOwner(ODatabaseDocumentTx.class);

        for (ORecordInternal<?> newRecord : newRecords) {
          assert !newRecord.getIdentity().isValid();

          if (databaseDocumentTx != null)
            databaseDocumentTx.save(newRecord);
          else
            database.save(newRecord);
        }
      } finally {
        addNewRecordsRecursivelly.set(true);
      }
    }

    switch (status) {
    case ORecordOperation.CREATED:
      database.callbackHooks(TYPE.BEFORE_CREATE, record);
      break;
    case ORecordOperation.LOADED:
      /**
       * Read hooks already invoked in {@link com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract#executeReadRecord}
       * .
       */
      break;
    case ORecordOperation.UPDATED:
      database.callbackHooks(TYPE.BEFORE_UPDATE, record);
      break;
    case ORecordOperation.DELETED:
      database.callbackHooks(TYPE.BEFORE_DELETE, record);
      break;
    }

    try {
      if (record.getIdentity().isTemporary())
        temp2persistent.put(record.getIdentity().copy(), record);

      if ((this.status == OTransaction.TXSTATUS.COMMITTING) && database.getStorage() instanceof OStorageEmbedded) {

        // I'M COMMITTING: BYPASS LOCAL BUFFER
        switch (status) {
        case ORecordOperation.CREATED:
        case ORecordOperation.UPDATED:
          database.executeSaveRecord(record, clusterName, record.getRecordVersion(), record.getRecordType(), false,
              OPERATION_MODE.SYNCHRONOUS, false, null, null);
          break;
        case ORecordOperation.DELETED:
          database.executeDeleteRecord(record, record.getRecordVersion(), false, false, OPERATION_MODE.SYNCHRONOUS, false);
          break;
        }

        final ORecordOperation txRecord = getRecordEntry(record.getIdentity());

        if (txRecord == null) {
          // NOT IN TX, SAVE IT ANYWAY
          allEntries.put(record.getIdentity(), new ORecordOperation(record, status));
        } else if (txRecord.record != record) {
          // UPDATE LOCAL RECORDS TO AVOID MISMATCH OF VERSION/CONTENT
          clusterName = getDatabase().getClusterNameById(record.getIdentity().getClusterId());
          if (!clusterName.equals(OMetadataDefault.CLUSTER_MANUAL_INDEX_NAME)
              && !clusterName.equals(OMetadataDefault.CLUSTER_INDEX_NAME))
            OLogManager
                .instance()
                .warn(
                    this,
                    "Found record in transaction with the same RID %s but different instance. Probably the record has been loaded from another transaction and reused on the current one: reload it from current transaction before to update or delete it",
                    record.getIdentity());

          txRecord.record = record;
          txRecord.type = status;
        }

      } else {
        final ORecordId rid = (ORecordId) record.getIdentity();

        if (!rid.isValid()) {
          record.onBeforeIdentityChanged(rid);

          // ASSIGN A UNIQUE SERIAL TEMPORARY ID
          if (rid.clusterId == ORID.CLUSTER_ID_INVALID)
            rid.clusterId = clusterName != null ? database.getClusterIdByName(clusterName) : database.getDefaultClusterId();
          rid.clusterPosition = OClusterPositionFactory.INSTANCE.valueOf(newObjectCounter--);

          record.onAfterIdentityChanged(record);
        } else
          // REMOVE FROM THE DB'S CACHE
          database.getLevel1Cache().freeRecord(rid);

        ORecordOperation txEntry = getRecordEntry(rid);

        if (txEntry == null) {
          if (!(rid.isTemporary() && status != ORecordOperation.CREATED)) {
            // NEW ENTRY: JUST REGISTER IT
            txEntry = new ORecordOperation(record, status);
            recordEntries.put(rid, txEntry);
          }
        } else {
          // UPDATE PREVIOUS STATUS
          txEntry.record = record;

          switch (txEntry.type) {
          case ORecordOperation.LOADED:
            switch (status) {
            case ORecordOperation.UPDATED:
              txEntry.type = ORecordOperation.UPDATED;
              break;
            case ORecordOperation.DELETED:
              txEntry.type = ORecordOperation.DELETED;
              break;
            }
            break;
          case ORecordOperation.UPDATED:
            switch (status) {
            case ORecordOperation.DELETED:
              txEntry.type = ORecordOperation.DELETED;
              break;
            }
            break;
          case ORecordOperation.DELETED:
            break;
          case ORecordOperation.CREATED:
            switch (status) {
            case ORecordOperation.DELETED:
              recordEntries.remove(rid);
              break;
            }
            break;
          }
        }
      }

      switch (status) {
      case ORecordOperation.CREATED:
        database.callbackHooks(TYPE.AFTER_CREATE, record);
        break;
      case ORecordOperation.LOADED:
        /**
         * Read hooks already invoked in
         * {@link com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract#executeReadRecord}.
         */
        break;
      case ORecordOperation.UPDATED:
        database.callbackHooks(TYPE.AFTER_UPDATE, record);
        break;
      case ORecordOperation.DELETED:
        database.callbackHooks(TYPE.AFTER_DELETE, record);
        break;
      }
    } catch (Throwable t) {
      switch (status) {
      case ORecordOperation.CREATED:
        database.callbackHooks(TYPE.CREATE_FAILED, record);
        break;
      case ORecordOperation.UPDATED:
        database.callbackHooks(TYPE.UPDATE_FAILED, record);
        break;
      case ORecordOperation.DELETED:
        database.callbackHooks(TYPE.DELETE_FAILED, record);
        break;
      }

      if (t instanceof RuntimeException)
        throw (RuntimeException) t;
      else
        throw new ODatabaseException("Error on saving record " + record.getIdentity(), t);
    }
  }

  private void extractAllNewNotEmbeddedRecords(Object value, Set<ORecordInternal<?>> newRecords, Set<Object> processedValues) {
    if (value == null)
      return;

    if (!(value instanceof ORecord || OMultiValue.isMultiValue(value.getClass())))
      return;

    if (!processedValues.add(value))
      return;

    if (value instanceof ORecord) {
      ORID rid = ((ORecord) value).getIdentity();

      if (!rid.isValid())
        newRecords.add((ORecordInternal<?>) value);

      if (value instanceof ODocument) {
        final ODocument document = (ODocument) value;
        final OClass documentClass = document.getSchemaClass();

        for (String fieldName : document.fieldNames()) {
          final Object fieldValue = document.field(fieldName);
          OType valueType = document.fieldType(fieldName);

          if (valueType == null && documentClass != null) {
            OProperty property = documentClass.getProperty(fieldName);
            if (property != null)
              valueType = property.getType();
          }

          if (valueType == null
              || !(valueType.equals(OType.EMBEDDED) || valueType.equals(OType.EMBEDDEDLIST) || valueType.equals(OType.EMBEDDEDMAP) || valueType
                  .equals(OType.EMBEDDEDSET)))
            extractAllNewNotEmbeddedRecords(fieldValue, newRecords, processedValues);
        }
      }
    } else if (OMultiValue.isMultiValue(value.getClass())) {
      for (Object collectionItem : OMultiValue.getMultiValueIterable(value))
        extractAllNewNotEmbeddedRecords(collectionItem, newRecords, processedValues);
    }
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

  public int getAutoRetries() {
    return autoRetries;
  }

  public void setAutoRetries(final int autoRetries) {
    this.autoRetries = autoRetries;
  }
}

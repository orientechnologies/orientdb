package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.client.remote.message.tx.IndexChange;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperation38Response;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializerDelta;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/** Created by tglman on 03/01/17. */
public class OTransactionOptimisticClient extends OTransactionOptimistic {

  private Set<String> indexChanged = new HashSet<>();

  public OTransactionOptimisticClient(ODatabaseDocumentInternal iDatabase) {
    super(iDatabase);
  }

  public void replaceContent(
      List<ORecordOperation38Response> operations, List<IndexChange> indexChanges) {

    Map<ORID, ORecordOperation> oldEntries = this.allEntries;
    this.allEntries = new LinkedHashMap<ORID, ORecordOperation>();
    int createCount = -2; // Start from -2 because temporary rids start from -2
    for (ORecordOperation38Response operation : operations) {
      if (!operation.getOldId().equals(operation.getId()))
        updatedRids.put(operation.getId().copy(), operation.getOldId());

      ORecord record = null;
      ORecordOperation op = oldEntries.get(operation.getOldId());
      if (op != null) {
        record = op.getRecord();
      }
      if (record == null) {
        getDatabase().getLocalCache().findRecord(operation.getOldId());
      }
      if (record != null) {
        record.unload();
      } else {
        record =
            Orient.instance()
                .getRecordFactoryManager()
                .newInstance(
                    operation.getRecordType(), operation.getOldId().getClusterId(), database);
      }
      if (operation.getType() == ORecordOperation.UPDATED
          && operation.getRecordType() == ODocument.RECORD_TYPE) {
        record.fromStream(operation.getOriginal());
        ODocumentSerializerDelta deltaSerializer = ODocumentSerializerDelta.instance();
        deltaSerializer.deserializeDelta(operation.getRecord(), (ODocument) record);
      } else {
        record.fromStream(operation.getRecord());
      }
      ORecordInternal.setIdentity(record, (ORecordId) operation.getId());
      ORecordInternal.setVersion(record, operation.getVersion());
      ORecordInternal.setContentChanged(record, operation.isContentChanged());
      getDatabase().getLocalCache().updateRecord(record);
      boolean callHook = checkCallHook(oldEntries, operation.getId(), operation.getType());
      addRecord(record, operation.getType(), null, callHook);
      if (operation.getType() == ORecordOperation.CREATED) createCount--;
    }
    newObjectCounter = createCount;

    for (IndexChange change : indexChanges) {
      NavigableMap<Object, OTransactionIndexChangesPerKey> changesPerKey =
          new TreeMap<>(ODefaultComparator.INSTANCE);
      for (Map.Entry<Object, OTransactionIndexChangesPerKey> keyChange :
          change.getKeyChanges().changesPerKey.entrySet()) {
        Object key = keyChange.getKey();
        if (key instanceof OIdentifiable && ((OIdentifiable) key).getIdentity().isNew())
          key = ((OIdentifiable) key).getRecord();
        OTransactionIndexChangesPerKey singleChange = new OTransactionIndexChangesPerKey(key);
        keyChange
            .getValue()
            .getEntriesAsList()
            .forEach(x -> singleChange.add(x.getValue(), x.getOperation()));
        changesPerKey.put(key, singleChange);
      }
      change.getKeyChanges().changesPerKey = changesPerKey;

      indexEntries.put(change.getName(), change.getKeyChanges());
    }
  }

  private boolean checkCallHook(Map<ORID, ORecordOperation> oldEntries, ORID rid, byte type) {
    ORecordOperation val = oldEntries.get(rid);
    return val == null || val.getType() != type;
  }

  public void addRecord(
      ORecord iRecord, final byte iStatus, final String iClusterName, boolean callHook) {
    if (iStatus != ORecordOperation.LOADED) changedDocuments.remove(iRecord);

    try {
      if (callHook) {
        switch (iStatus) {
          case ORecordOperation.CREATED:
            {
              OIdentifiable res = database.beforeCreateOperations(iRecord, iClusterName);
              if (res != null) {
                iRecord = (ORecord) res;
                changed = true;
              }
            }
            break;
          case ORecordOperation.LOADED:
            /**
             * Read hooks already invoked in {@link
             * com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx#executeReadRecord}
             */
            break;
          case ORecordOperation.UPDATED:
            {
              OIdentifiable res = database.beforeUpdateOperations(iRecord, iClusterName);
              if (res != null) {
                iRecord = (ORecord) res;
                changed = true;
              }
            }
            break;

          case ORecordOperation.DELETED:
            database.beforeDeleteOperations(iRecord, iClusterName);
            break;
        }
      }
      try {
        final ORecordId rid = (ORecordId) iRecord.getIdentity();
        ORecordOperation txEntry = getRecordEntry(rid);

        if (txEntry == null) {
          if (!(rid.isTemporary() && iStatus != ORecordOperation.CREATED)) {
            // NEW ENTRY: JUST REGISTER IT
            txEntry = new ORecordOperation(iRecord, iStatus);
            allEntries.put(rid.copy(), txEntry);
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
                  allEntries.remove(rid);
                  // txEntry.type = ORecordOperation.DELETED;
                  break;
              }
              break;
          }
        }
        if (callHook) {
          switch (iStatus) {
            case ORecordOperation.CREATED:
              database.callbackHooks(ORecordHook.TYPE.AFTER_CREATE, iRecord);
              break;
            case ORecordOperation.LOADED:
              /**
               * Read hooks already invoked in {@link
               * com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx#executeReadRecord}
               * .
               */
              break;
            case ORecordOperation.UPDATED:
              database.callbackHooks(ORecordHook.TYPE.AFTER_UPDATE, iRecord);
              break;
            case ORecordOperation.DELETED:
              database.callbackHooks(ORecordHook.TYPE.AFTER_DELETE, iRecord);
              break;
          }
        }
      } catch (Exception e) {
        if (callHook) {
          switch (iStatus) {
            case ORecordOperation.CREATED:
              database.callbackHooks(ORecordHook.TYPE.CREATE_FAILED, iRecord);
              break;
            case ORecordOperation.UPDATED:
              database.callbackHooks(ORecordHook.TYPE.UPDATE_FAILED, iRecord);
              break;
            case ORecordOperation.DELETED:
              database.callbackHooks(ORecordHook.TYPE.DELETE_FAILED, iRecord);
              break;
          }
        }

        throw OException.wrapException(
            new ODatabaseException("Error on saving record " + iRecord.getIdentity()), e);
      }
    } finally {
      if (callHook) {
        switch (iStatus) {
          case ORecordOperation.CREATED:
            database.callbackHooks(ORecordHook.TYPE.FINALIZE_CREATION, iRecord);
            break;
          case ORecordOperation.UPDATED:
            database.callbackHooks(ORecordHook.TYPE.FINALIZE_UPDATE, iRecord);
            break;
          case ORecordOperation.DELETED:
            database.callbackHooks(ORecordHook.TYPE.FINALIZE_DELETION, iRecord);
            break;
        }
      }
    }
  }

  public Set<String> getIndexChanged() {
    return indexChanged;
  }

  public void addIndexChanged(String indexName) {
    indexChanged.add(indexName);
  }
}

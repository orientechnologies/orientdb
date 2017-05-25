package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.client.remote.message.tx.IndexChange;
import com.orientechnologies.orient.client.remote.message.tx.ORecordOperationRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;

import java.util.*;

/**
 * Created by tglman on 03/01/17.
 */
public class OTransactionOptimisticClient extends OTransactionOptimistic {

  public OTransactionOptimisticClient(ODatabaseDocumentInternal iDatabase) {
    super(iDatabase);
  }

  public void replaceContent(List<ORecordOperationRequest> operations, List<IndexChange> indexChanges) {

    Map<ORID, ORecordOperation> oldEntries = this.allEntries;
    this.allEntries = new LinkedHashMap<ORID, ORecordOperation>();

    for (ORecordOperationRequest operation : operations) {
      if (!operation.getOldId().equals(operation.getId()))
        updatedRids.put(operation.getId(), operation.getOldId());
      ORecordInternal.setIdentity(operation.getRecord(), (ORecordId) operation.getOldId());
      ORecordInternal.setVersion(operation.getRecord(), operation.getVersion());
      boolean callHook = checkCallHook(oldEntries, operation.getOldId(), operation.getType());
      addRecord(operation.getRecord(), operation.getType(), null, callHook);
    }

    for (IndexChange change : indexChanges) {
      NavigableMap<Object, OTransactionIndexChangesPerKey> changesPerKey = new TreeMap<>(ODefaultComparator.INSTANCE);
      for (Map.Entry<Object, OTransactionIndexChangesPerKey> keyChange : change.getKeyChanges().changesPerKey.entrySet()) {
        Object key = keyChange.getKey();
        if (key instanceof OIdentifiable && ((OIdentifiable) key).getIdentity().isNew())
          key = ((OIdentifiable) key).getRecord();
        OTransactionIndexChangesPerKey singleChange = new OTransactionIndexChangesPerKey(key);
        singleChange.entries.addAll(keyChange.getValue().entries);
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

  public void addRecord(final ORecord iRecord, final byte iStatus, final String iClusterName, boolean callHook) {
    if (iStatus != ORecordOperation.LOADED)
      changedDocuments.remove(iRecord);

    try {
      if (callHook) {
        switch (iStatus) {
        case ORecordOperation.CREATED: {
          ORecordHook.RESULT res = database.callbackHooks(ORecordHook.TYPE.BEFORE_CREATE, iRecord);
          if (res == ORecordHook.RESULT.RECORD_CHANGED && iRecord instanceof ODocument) {
            ((ODocument) iRecord).validate();
            changed = true;
          }
        }
        break;
        case ORecordOperation.LOADED:
          /**
           * Read hooks already invoked in {@link com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx#executeReadRecord}
           */
          break;
        case ORecordOperation.UPDATED: {
          database.checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_UPDATE, iClusterName);
          ORecordHook.RESULT res = database.callbackHooks(ORecordHook.TYPE.BEFORE_UPDATE, iRecord);
          if (res == ORecordHook.RESULT.RECORD_CHANGED && iRecord instanceof ODocument)
            ((ODocument) iRecord).validate();
        }
        break;

        case ORecordOperation.DELETED:
          database.checkSecurity(ORule.ResourceGeneric.CLUSTER, ORole.PERMISSION_DELETE, iClusterName);
          database.callbackHooks(ORecordHook.TYPE.BEFORE_DELETE, iRecord);
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
             * Read hooks already invoked in
             * {@link com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx#executeReadRecord} .
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

        throw OException.wrapException(new ODatabaseException("Error on saving record " + iRecord.getIdentity()), e);
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

//  public OTransactionIndexChanges getIndexChangesInternal(final String iIndexName) {
//    return null;
//  }

}

package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.index.OClassIndexManager;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.query.live.OLiveQueryHookV2;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;

import java.util.ArrayList;
import java.util.List;

public class OTransactionOptimisticDistributed extends OTransactionOptimistic {
  public OTransactionOptimisticDistributed(ODatabaseDocumentInternal database, List<ORecordOperation> changes) {
    super(database);
    for (ORecordOperation change : changes) {
      allEntries.put(change.getRID(), change);
      resolveTracking(change);
    }
  }

  private void resolveTracking(ORecordOperation change) {
    if (change.getRecord() instanceof ODocument) {
      ODocument rec = (ODocument) change.getRecord();
      List<OClassIndexManager.IndexChange> changes = new ArrayList<>();
      switch (change.getType()) {
      case ORecordOperation.CREATED:
        OClassIndexManager.processIndexOnCreate(database, rec, changes);
        if (change.getRecord() instanceof ODocument) {
          OLiveQueryHook.addOp((ODocument) change.getRecord(), ORecordOperation.CREATED, database);
          OLiveQueryHookV2.addOp((ODocument) change.getRecord(), ORecordOperation.CREATED, database);
        }
        break;
      case ORecordOperation.UPDATED:
        OClassIndexManager.processIndexOnUpdate(database, rec, changes);
        if (change.getRecord() instanceof ODocument) {
          OLiveQueryHook.addOp((ODocument) change.getRecord(), ORecordOperation.UPDATED, database);
          OLiveQueryHookV2.addOp((ODocument) change.getRecord(), ORecordOperation.UPDATED, database);
        }
        break;
      case ORecordOperation.DELETED:
        OClassIndexManager.processIndexOnDelete(database, rec, changes);
        if (change.getRecord() instanceof ODocument) {
          OLiveQueryHook.addOp((ODocument) change.getRecord(), ORecordOperation.DELETED, database);
          OLiveQueryHookV2.addOp((ODocument) change.getRecord(), ORecordOperation.DELETED, database);
        }
        break;
      case ORecordOperation.LOADED:
        break;
      default:
        break;
      }
      for (OClassIndexManager.IndexChange indexChange : changes) {
        addIndexEntry(indexChange.index, indexChange.index.getName(), indexChange.operation, indexChange.key, indexChange.value);
      }

    }
  }
}

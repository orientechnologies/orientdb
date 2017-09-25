package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OClassIndexManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;

import java.util.ArrayList;
import java.util.List;

public class OTransactionOptimisticDistributed extends OTransactionOptimistic {
  public OTransactionOptimisticDistributed(ODatabaseDocumentInternal database, List<ORecordOperation> changes) {
    super(database);
    for (ORecordOperation change : changes) {
      allEntries.put(change.getRID(), change);
      resolveIndexes(change);
    }
  }

  private void resolveIndexes(ORecordOperation change) {
    if (change.getRecord() instanceof ODocument) {
      ODocument rec = (ODocument) change.getRecord();
      List<OClassIndexManager.IndexChange> changes = new ArrayList<>();
      switch (change.getType()) {
      case ORecordOperation.CREATED:
        OClassIndexManager.processIndexOnCreate(rec, changes);
        break;
      case ORecordOperation.UPDATED:
        OClassIndexManager.processIndexOnUpdate(rec, changes);
        break;
      case ORecordOperation.DELETED:
        OClassIndexManager.processIndexOnDelete(rec, changes);
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

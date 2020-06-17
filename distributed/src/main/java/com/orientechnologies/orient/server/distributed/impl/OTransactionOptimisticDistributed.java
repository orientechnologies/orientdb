package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import java.util.List;

public class OTransactionOptimisticDistributed extends OTransactionOptimistic {
  private List<ORecordOperation> changes;

  public OTransactionOptimisticDistributed(
      ODatabaseDocumentInternal database, List<ORecordOperation> changes) {
    super(database);
    this.changes = changes;
  }

  @Override
  public void begin() {
    super.begin();
    for (ORecordOperation change : changes) {
      allEntries.put(change.getRID(), change);
      resolveTracking(change);
    }
  }

  public void setDatabase(ODatabaseDocumentInternal database) {
    this.database = database;
  }
}

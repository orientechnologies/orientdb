package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;

import java.util.List;

public class OTransactionOptimisticDistributed extends OTransactionOptimistic {
  public OTransactionOptimisticDistributed(ODatabaseDocumentInternal database, List<ORecordOperation> changes) {
    super(database);
    for (ORecordOperation change : changes) {
      allEntries.put(change.getRID(), change);
      //TODO: Calcolate Indexes
    }
  }
}

package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.server.distributed.impl.task.OLockKeySource;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1Task;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionUniqueKey;
import java.util.SortedSet;
import java.util.TreeSet;

public final class OLocalKeySource implements OLockKeySource {
  private final OTransactionId txId;
  private final OTransactionInternal iTx;
  private final ODatabaseDocumentDistributed database;

  public OLocalKeySource(
      OTransactionId txId, OTransactionInternal iTx, ODatabaseDocumentDistributed database) {
    this.txId = txId;
    this.iTx = iTx;
    this.database = database;
  }

  @Override
  public SortedSet<OTransactionUniqueKey> getUniqueKeys() {
    TreeSet<OTransactionUniqueKey> uniqueIndexKeys = new TreeSet<>();
    iTx.getIndexOperations()
        .forEach(
            (index, changes) -> {
              OIndexInternal resolvedIndex =
                  changes.resolveAssociatedIndex(
                      index, database.getMetadata().getIndexManagerInternal(), database);
              if (resolvedIndex.isUnique()) {
                for (Object keyWithChange : changes.changesPerKey.keySet()) {
                  Object keyChange = OTransactionPhase1Task.mapKey(keyWithChange);
                  uniqueIndexKeys.add(new OTransactionUniqueKey(index, keyChange, 0));
                }
                if (!changes.nullKeyChanges.isEmpty()) {
                  uniqueIndexKeys.add(new OTransactionUniqueKey(index, null, 0));
                }
              }
            });
    return uniqueIndexKeys;
  }

  @Override
  public OTransactionId getTransactionId() {
    return txId;
  }

  @Override
  public SortedSet<ORID> getRids() {
    SortedSet<ORID> set = new TreeSet<ORID>();
    for (ORecordOperation operation : iTx.getRecordOperations()) {
      OTransactionPhase1Task.mapRid(set, operation);
    }
    return set;
  }
}

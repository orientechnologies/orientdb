package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.server.distributed.impl.task.OLockKeySource;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

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
  public SortedSet<OPair<String, Object>> getUniqueKeys() {
    TreeSet<OPair<String, Object>> uniqueIndexKeys = new TreeSet<>();
    iTx.getIndexOperations()
        .forEach(
            (index, changes) -> {
              if (changes
                  .resolveAssociatedIndex(
                      index, database.getMetadata().getIndexManagerInternal(), database)
                  .isUnique()) {
                for (Object keyWithChange : changes.changesPerKey.keySet()) {
                  uniqueIndexKeys.add(new OPair<>(index, keyWithChange));
                }
                if (!changes.nullKeyChanges.entries.isEmpty()) {
                  uniqueIndexKeys.add(new OPair<>(index, null));
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
    return new TreeSet<ORID>(
        iTx.getRecordOperations().stream()
            .map((x) -> x.getRID().copy())
            .collect(Collectors.toSet()));
  }
}

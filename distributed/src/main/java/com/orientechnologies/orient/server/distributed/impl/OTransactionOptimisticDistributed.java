package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTransactionUniqueKey;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

public class OTransactionOptimisticDistributed extends OTransactionOptimistic {
  private List<ORecordOperation> changes;
  private Map<String, Map<Object, Integer>> keyVersions;

  public OTransactionOptimisticDistributed(
      ODatabaseDocumentInternal database,
      List<ORecordOperation> changes,
      SortedSet<OTransactionUniqueKey> uniqueIndexKeys) {
    super(database);
    this.changes = changes;
    keyVersions = new HashMap<String, Map<Object, Integer>>();
    for (OTransactionUniqueKey key : uniqueIndexKeys) {
      Map<Object, Integer> index = keyVersions.get(key.getIndex());
      if (index == null) {
        index = new HashMap<Object, Integer>();
        keyVersions.put(key.getIndex(), index);
      }
      index.put(key.getKey(), key.getVersion());
    }
  }

  @Override
  public void begin() {
    super.begin();
    for (ORecordOperation change : changes) {
      allEntries.put(change.getRID(), change);
      // TODO: check that the resolve tracking is executed after the validation of the record
      // versions
      resolveTracking(change);
    }
  }

  public void setDatabase(ODatabaseDocumentInternal database) {
    this.database = database;
  }

  public int getVersionForKey(final String index, final Object key) {
    final Map<Object, Integer> indexKeys = keyVersions.get(index);
    assert indexKeys != null;
    return indexKeys.get(key);
  }
}

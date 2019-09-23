package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpage;

import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

public final class ClusterPageInitPO extends PageOperationRecord {
  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OClusterPage clusterPage = new OClusterPage(cacheEntry);
    clusterPage.init();
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    //do nothing
  }

  @Override
  public int getId() {
    return WALRecordTypes.CLUSTER_PAGE_INIT_PO;
  }
}

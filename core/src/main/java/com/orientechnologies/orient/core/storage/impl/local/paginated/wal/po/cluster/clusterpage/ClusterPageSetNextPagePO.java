package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

import java.nio.ByteBuffer;

public final class ClusterPageSetNextPagePO extends PageOperationRecord {
  private int nextPage;
  private int oldNextPage;

  public ClusterPageSetNextPagePO() {
  }

  public ClusterPageSetNextPagePO(int nextPage, int oldNextPage) {
    this.nextPage = nextPage;
    this.oldNextPage = oldNextPage;
  }

  public int getNextPage() {
    return nextPage;
  }

  public int getOldNextPage() {
    return oldNextPage;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OClusterPage clusterPage = new OClusterPage(cacheEntry);
    clusterPage.setNextPage(nextPage);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OClusterPage clusterPage = new OClusterPage(cacheEntry);
    clusterPage.setNextPage(oldNextPage);
  }

  @Override
  public int getId() {
    return WALRecordTypes.CLUSTER_PAGE_SET_NEXT_PAGE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(nextPage);
    buffer.putInt(oldNextPage);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    nextPage = buffer.getInt();
    oldNextPage = buffer.getInt();
  }
}

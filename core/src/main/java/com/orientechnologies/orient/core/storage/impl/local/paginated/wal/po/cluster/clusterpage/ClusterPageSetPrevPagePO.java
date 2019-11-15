package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

import java.nio.ByteBuffer;

public final class ClusterPageSetPrevPagePO extends PageOperationRecord {
  private int oldPrevPage;
  private int prevPage;

  public ClusterPageSetPrevPagePO() {
  }

  public ClusterPageSetPrevPagePO(int oldPrevPage, int prevPage) {
    this.oldPrevPage = oldPrevPage;
    this.prevPage = prevPage;
  }

  public int getOldPrevPage() {
    return oldPrevPage;
  }

  public int getPrevPage() {
    return prevPage;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OClusterPage page = new OClusterPage(cacheEntry);
    page.setPrevPage(prevPage);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OClusterPage page = new OClusterPage(cacheEntry);
    page.setPrevPage(oldPrevPage);
  }

  @Override
  public int getId() {
    return WALRecordTypes.CLUSTER_PAGE_SET_PREV_PAGE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(oldPrevPage);
    buffer.putInt(prevPage);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    oldPrevPage = buffer.getInt();
    prevPage = buffer.getInt();
  }
}

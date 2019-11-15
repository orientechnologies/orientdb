package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v0.paginatedclusterstate;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.v0.OPaginatedClusterStateV0;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

import java.nio.ByteBuffer;

public final class PaginatedClusterStateV0SetFreeListPagePO extends PageOperationRecord {
  private int index;

  private int oldPageIndex;
  private int newPageIndex;

  public PaginatedClusterStateV0SetFreeListPagePO() {
  }

  public PaginatedClusterStateV0SetFreeListPagePO(int index, int oldPageIndex, int newPageIndex) {
    this.index = index;
    this.oldPageIndex = oldPageIndex;
    this.newPageIndex = newPageIndex;
  }

  public int getIndex() {
    return index;
  }

  public int getOldPageIndex() {
    return oldPageIndex;
  }

  public int getNewPageIndex() {
    return newPageIndex;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OPaginatedClusterStateV0 paginatedClusterStateV0 = new OPaginatedClusterStateV0(cacheEntry);
    paginatedClusterStateV0.setFreeListPage(index, newPageIndex);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OPaginatedClusterStateV0 paginatedClusterStateV0 = new OPaginatedClusterStateV0(cacheEntry);
    paginatedClusterStateV0.setFreeListPage(index, oldPageIndex);
  }

  @Override
  public int getId() {
    return WALRecordTypes.PAGINATED_CLUSTER_STATE_V0_SET_FREE_LIST_PAGE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(index);

    buffer.putInt(oldPageIndex);
    buffer.putInt(newPageIndex);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    index = buffer.getInt();

    oldPageIndex = buffer.getInt();
    newPageIndex = buffer.getInt();
  }
}

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v1.paginatedclusterstate;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.v1.OPaginatedClusterStateV1;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

import java.nio.ByteBuffer;

public final class PaginatedClusterStateV1SetFreeListPagePO extends PageOperationRecord {
  private int index;

  private int oldPageIndex;
  private int newPageIndex;

  public PaginatedClusterStateV1SetFreeListPagePO() {
  }

  public PaginatedClusterStateV1SetFreeListPagePO(int index, int oldPageIndex, int newPageIndex) {
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
    final OPaginatedClusterStateV1 paginatedClusterStateV1 = new OPaginatedClusterStateV1(cacheEntry);
    paginatedClusterStateV1.setFreeListPage(index, newPageIndex);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OPaginatedClusterStateV1 paginatedClusterStateV1 = new OPaginatedClusterStateV1(cacheEntry);
    paginatedClusterStateV1.setFreeListPage(index, oldPageIndex);
  }

  @Override
  public int getId() {
    return WALRecordTypes.PAGINATED_CLUSTER_STATE_V1_SET_FREE_LIST_PAGE_PO;
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

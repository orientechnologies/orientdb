package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v1.paginatedclusterstate;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.v1.OPaginatedClusterStateV1;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

import java.nio.ByteBuffer;

public final class PaginatedClusterStateV1SetSizePO extends PageOperationRecord {
  private int oldSize;
  private int newSize;

  public PaginatedClusterStateV1SetSizePO() {
  }

  public PaginatedClusterStateV1SetSizePO(int oldSize, int newSize) {
    this.oldSize = oldSize;
    this.newSize = newSize;
  }

  public long getOldSize() {
    return oldSize;
  }

  public long getNewSize() {
    return newSize;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OPaginatedClusterStateV1 paginatedClusterStateV1 = new OPaginatedClusterStateV1(cacheEntry);
    paginatedClusterStateV1.setSize(newSize);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OPaginatedClusterStateV1 paginatedClusterStateV1 = new OPaginatedClusterStateV1(cacheEntry);
    paginatedClusterStateV1.setSize(oldSize);
  }

  @Override
  public int getId() {
    return WALRecordTypes.PAGINATED_CLUSTER_STATE_V1_SET_SIZE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(oldSize);
    buffer.putInt(newSize);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    oldSize = buffer.getInt();
    newSize = buffer.getInt();
  }
}

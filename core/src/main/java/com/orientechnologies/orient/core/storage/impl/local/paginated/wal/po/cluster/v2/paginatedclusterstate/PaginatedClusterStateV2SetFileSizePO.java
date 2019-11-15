package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v2.paginatedclusterstate;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.v2.OPaginatedClusterStateV2;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

import java.nio.ByteBuffer;

public final class PaginatedClusterStateV2SetFileSizePO extends PageOperationRecord {
  private int oldFileSize;
  private int newFileSize;

  public PaginatedClusterStateV2SetFileSizePO() {
  }

  public PaginatedClusterStateV2SetFileSizePO(int oldFileSize, int newFileSize) {
    this.oldFileSize = oldFileSize;
    this.newFileSize = newFileSize;
  }

  public int getOldFileSize() {
    return oldFileSize;
  }

  public int getNewFileSize() {
    return newFileSize;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OPaginatedClusterStateV2 paginatedClusterStateV2 = new OPaginatedClusterStateV2(cacheEntry);
    paginatedClusterStateV2.setFileSize(newFileSize);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OPaginatedClusterStateV2 paginatedClusterStateV2 = new OPaginatedClusterStateV2(cacheEntry);
    paginatedClusterStateV2.setFileSize(oldFileSize);
  }

  @Override
  public int getId() {
    return WALRecordTypes.PAGINATED_CLUSTER_STATE_V2_SET_FILE_SIZE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(oldFileSize);
    buffer.putInt(newFileSize);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    oldFileSize = buffer.getInt();
    newFileSize = buffer.getInt();
  }
}

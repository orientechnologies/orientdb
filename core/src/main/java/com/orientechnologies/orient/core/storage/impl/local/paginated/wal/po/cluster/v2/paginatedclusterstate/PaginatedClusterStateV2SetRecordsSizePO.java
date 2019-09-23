package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v2.paginatedclusterstate;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.v2.OPaginatedClusterStateV2;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

import java.nio.ByteBuffer;

public final class PaginatedClusterStateV2SetRecordsSizePO extends PageOperationRecord {
  private int oldRecordsSize;
  private int newRecordsSize;

  public PaginatedClusterStateV2SetRecordsSizePO() {
  }

  public PaginatedClusterStateV2SetRecordsSizePO(int oldRecordsSize, int newRecordsSize) {
    this.oldRecordsSize = oldRecordsSize;
    this.newRecordsSize = newRecordsSize;
  }

  public long getOldRecordsSize() {
    return oldRecordsSize;
  }

  public long getNewRecordsSize() {
    return newRecordsSize;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OPaginatedClusterStateV2 paginatedClusterStateV2 = new OPaginatedClusterStateV2(cacheEntry);
    paginatedClusterStateV2.setRecordsSize(newRecordsSize);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OPaginatedClusterStateV2 paginatedClusterStateV2 = new OPaginatedClusterStateV2(cacheEntry);
    paginatedClusterStateV2.setRecordsSize(oldRecordsSize);
  }

  @Override
  public int getId() {
    return WALRecordTypes.PAGINATED_CLUSTER_STATE_V2_SET_RECORDS_SIZE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(oldRecordsSize);
    buffer.putInt(newRecordsSize);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    oldRecordsSize = buffer.getInt();
    newRecordsSize = buffer.getInt();
  }
}

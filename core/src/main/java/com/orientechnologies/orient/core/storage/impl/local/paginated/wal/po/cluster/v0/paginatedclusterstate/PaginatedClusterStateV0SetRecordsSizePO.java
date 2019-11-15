package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v0.paginatedclusterstate;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.v0.OPaginatedClusterStateV0;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

import java.nio.ByteBuffer;

public final class PaginatedClusterStateV0SetRecordsSizePO extends PageOperationRecord {
  private long oldRecordsSize;
  private long newRecordsSize;

  public PaginatedClusterStateV0SetRecordsSizePO() {
  }

  public PaginatedClusterStateV0SetRecordsSizePO(long oldRecordsSize, long newRecordsSize) {
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
    final OPaginatedClusterStateV0 paginatedClusterStateV0 = new OPaginatedClusterStateV0(cacheEntry);
    paginatedClusterStateV0.setRecordsSize(newRecordsSize);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OPaginatedClusterStateV0 paginatedClusterStateV0 = new OPaginatedClusterStateV0(cacheEntry);
    paginatedClusterStateV0.setRecordsSize(oldRecordsSize);
  }

  @Override
  public int getId() {
    return WALRecordTypes.PAGINATED_CLUSTER_STATE_V0_SET_RECORDS_SIZE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OLongSerializer.LONG_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putLong(oldRecordsSize);
    buffer.putLong(newRecordsSize);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    oldRecordsSize = buffer.getLong();
    newRecordsSize = buffer.getLong();
  }
}

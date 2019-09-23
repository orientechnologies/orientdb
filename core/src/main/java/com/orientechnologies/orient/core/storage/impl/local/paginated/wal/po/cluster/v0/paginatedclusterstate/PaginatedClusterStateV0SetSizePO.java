package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v0.paginatedclusterstate;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.v0.OPaginatedClusterStateV0;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

import java.nio.ByteBuffer;

public final class PaginatedClusterStateV0SetSizePO extends PageOperationRecord {
  private long oldSize;
  private long newSize;

  public PaginatedClusterStateV0SetSizePO() {
  }

  public PaginatedClusterStateV0SetSizePO(long oldSize, long newSize) {
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
    final OPaginatedClusterStateV0 paginatedClusterStateV0 = new OPaginatedClusterStateV0(cacheEntry);
    paginatedClusterStateV0.setSize(newSize);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OPaginatedClusterStateV0 paginatedClusterStateV0 = new OPaginatedClusterStateV0(cacheEntry);
    paginatedClusterStateV0.setSize(oldSize);
  }

  @Override
  public int getId() {
    return WALRecordTypes.PAGINATED_CLUSTER_STATE_V0_SET_SIZE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OLongSerializer.LONG_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putLong(oldSize);
    buffer.putLong(newSize);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    oldSize = buffer.getLong();
    newSize = buffer.getLong();
  }
}

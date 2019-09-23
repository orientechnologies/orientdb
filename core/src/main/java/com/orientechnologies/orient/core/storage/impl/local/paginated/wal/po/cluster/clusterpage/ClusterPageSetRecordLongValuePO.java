package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

import java.nio.ByteBuffer;

public final class ClusterPageSetRecordLongValuePO extends PageOperationRecord {
  private int recordPosition;
  private int offset;

  private long value;
  private long oldValue;

  public ClusterPageSetRecordLongValuePO() {
  }

  public ClusterPageSetRecordLongValuePO(int recordPosition, int offset, long value, long oldValue) {
    this.recordPosition = recordPosition;
    this.offset = offset;
    this.value = value;
    this.oldValue = oldValue;
  }

  public int getRecordPosition() {
    return recordPosition;
  }

  public int getOffset() {
    return offset;
  }

  public long getValue() {
    return value;
  }

  public long getOldValue() {
    return oldValue;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OClusterPage clusterPage = new OClusterPage(cacheEntry);
    clusterPage.setRecordLongValue(recordPosition, offset, value);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OClusterPage clusterPage = new OClusterPage(cacheEntry);
    clusterPage.setRecordLongValue(recordPosition, offset, oldValue);
  }

  @Override
  public int getId() {
    return WALRecordTypes.CLUSTER_PAGE_SET_RECORD_LONG_VALUE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE + 2 * OLongSerializer.LONG_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(recordPosition);
    buffer.putInt(offset);

    buffer.putLong(value);
    buffer.putLong(oldValue);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    recordPosition = buffer.getInt();
    offset = buffer.getInt();

    value = buffer.getLong();
    oldValue = buffer.getLong();
  }
}

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

import java.nio.ByteBuffer;
import java.util.Collections;

public final class ClusterPageAppendRecordPO extends PageOperationRecord {
  private int     recordVersion;
  private byte[]  record;
  private int     requestedPosition;
  private int     recordPosition;
  private boolean allocatedFromFreeList;

  public ClusterPageAppendRecordPO() {
  }

  public int getRecordVersion() {
    return recordVersion;
  }

  public byte[] getRecord() {
    return record;
  }

  public int getRequestedPosition() {
    return requestedPosition;
  }

  public int getRecordPosition() {
    return recordPosition;
  }

  public boolean isAllocatedFromFreeList() {
    return allocatedFromFreeList;
  }

  public ClusterPageAppendRecordPO(int recordVersion, byte[] record, int requestedPosition, int recordPosition,
      boolean allocatedFromFreeList) {
    this.recordVersion = recordVersion;
    this.record = record;
    this.requestedPosition = requestedPosition;
    this.recordPosition = recordPosition;
    this.allocatedFromFreeList = allocatedFromFreeList;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OClusterPage clusterPage = new OClusterPage(cacheEntry);
    int allocatedPosition;
    if (requestedPosition < 0) {
      allocatedPosition = clusterPage.appendRecord(recordVersion, record, recordPosition, Collections.emptySet());
    } else {
      allocatedPosition = clusterPage.appendRecord(recordVersion, record, requestedPosition, Collections.emptySet());
    }

    if (allocatedPosition < 0) {
      throw new IllegalStateException("Can not redo operation of record creation");
    }
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OClusterPage clusterPage = new OClusterPage(cacheEntry);
    final byte[] deletedRecord = clusterPage.deleteRecord(recordPosition, allocatedFromFreeList);
    if (deletedRecord == null) {
      throw new IllegalStateException("Can not undo operation of record creation");
    }
  }

  @Override
  public int getId() {
    return WALRecordTypes.CLUSTER_PAGE_APPEND_RECORD_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 4 * OIntegerSerializer.INT_SIZE + record.length + OByteSerializer.BYTE_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.put(allocatedFromFreeList ? 1 : (byte) 0);

    buffer.putInt(recordVersion);
    buffer.putInt(requestedPosition);
    buffer.putInt(recordPosition);

    buffer.putInt(record.length);
    buffer.put(record);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    allocatedFromFreeList = buffer.get() > 0;

    recordVersion = buffer.getInt();
    requestedPosition = buffer.getInt();
    recordPosition = buffer.getInt();

    final int recordLen = buffer.getInt();
    record = new byte[recordLen];
    buffer.get(record);
  }
}

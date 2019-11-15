package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

import java.nio.ByteBuffer;
import java.util.Collections;

public final class ClusterPageDeleteRecordPO extends PageOperationRecord {
  private int     recordPosition;
  private int     recordVersion;
  private byte[]  record;
  private boolean preserveFreeListPointer;

  public ClusterPageDeleteRecordPO() {
  }

  public ClusterPageDeleteRecordPO(int recordPosition, int recordVersion, byte[] record, boolean preserveFreeListPointer) {
    this.recordPosition = recordPosition;
    this.recordVersion = recordVersion;
    this.record = record;
    this.preserveFreeListPointer = preserveFreeListPointer;
  }

  public int getRecordPosition() {
    return recordPosition;
  }

  public int getRecordVersion() {
    return recordVersion;
  }

  public byte[] getRecord() {
    return record;
  }

  public boolean isPreserveFreeListPointer() {
    return preserveFreeListPointer;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OClusterPage clusterPage = new OClusterPage(cacheEntry);
    final byte[] deletedRecord = clusterPage.deleteRecord(recordPosition, preserveFreeListPointer);
    if (deletedRecord == null) {
      throw new IllegalStateException("Can not redo operation of record deletion");
    }
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OClusterPage clusterPage = new OClusterPage(cacheEntry);
    final int allocatedPosition = clusterPage.appendRecord(recordVersion, record, recordPosition, Collections.emptySet());
    if (allocatedPosition < 0) {
      throw new IllegalStateException("Can not undo operation of record creation.");
    }
  }

  @Override
  public int getId() {
    return WALRecordTypes.CLUSTER_PAGE_DELETE_RECORD_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OByteSerializer.BYTE_SIZE + 3 * OIntegerSerializer.INT_SIZE + record.length;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.put(preserveFreeListPointer ? 1 : (byte) 0);

    buffer.putInt(recordPosition);
    buffer.putInt(recordVersion);

    buffer.putInt(record.length);
    buffer.put(record);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    preserveFreeListPointer = buffer.get() != 0;

    recordPosition = buffer.getInt();
    recordVersion = buffer.getInt();

    final int recLen = buffer.getInt();
    record = new byte[recLen];
    buffer.get(record);
  }
}

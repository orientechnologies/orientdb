package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

import java.nio.ByteBuffer;

public final class ClusterPageReplaceRecordPO extends PageOperationRecord {
  private int recordPosition;

  private int    recordVersion;
  private byte[] record;

  private int    oldRecordVersion;
  private byte[] oldRecord;

  public ClusterPageReplaceRecordPO() {
  }

  public ClusterPageReplaceRecordPO(int recordPosition, int recordVersion, byte[] record, int oldRecordVersion, byte[] oldRecord) {
    this.recordPosition = recordPosition;
    this.recordVersion = recordVersion;
    this.record = record;
    this.oldRecord = oldRecord;
    this.oldRecordVersion = oldRecordVersion;
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

  public byte[] getOldRecord() {
    return oldRecord;
  }

  public int getOldRecordVersion() {
    return oldRecordVersion;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OClusterPage clusterPage = new OClusterPage(cacheEntry);
    clusterPage.replaceRecord(recordPosition, record, recordVersion);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OClusterPage clusterPage = new OClusterPage(cacheEntry);
    clusterPage.replaceRecord(recordPosition, oldRecord, oldRecordVersion);
  }

  @Override
  public int getId() {
    return WALRecordTypes.CLUSTER_PAGE_REPLACE_RECORD_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 5 * OIntegerSerializer.INT_SIZE + record.length + oldRecord.length;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(recordPosition);

    buffer.putInt(recordVersion);
    buffer.putInt(record.length);
    buffer.put(record);

    buffer.putInt(oldRecordVersion);
    buffer.putInt(oldRecord.length);
    buffer.put(oldRecord);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    recordPosition = buffer.getInt();

    recordVersion = buffer.getInt();
    int recLen = buffer.getInt();
    record = new byte[recLen];
    buffer.get(record);

    oldRecordVersion = buffer.getInt();
    recLen = buffer.getInt();
    oldRecord = new byte[recLen];
    buffer.get(oldRecord);
  }
}

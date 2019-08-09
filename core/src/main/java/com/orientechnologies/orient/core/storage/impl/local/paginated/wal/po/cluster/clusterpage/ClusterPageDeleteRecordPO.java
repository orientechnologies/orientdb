package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

import java.nio.ByteBuffer;
import java.util.Collections;

public class ClusterPageDeleteRecordPO extends PageOperationRecord {
  private int    recordPosition;
  private int    recordVersion;
  private byte[] record;

  public ClusterPageDeleteRecordPO() {
  }

  public ClusterPageDeleteRecordPO(int recordPosition, int recordVersion, byte[] record) {
    this.recordPosition = recordPosition;
    this.recordVersion = recordVersion;
    this.record = record;
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

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OClusterPage clusterPage = new OClusterPage(cacheEntry);
    clusterPage.deleteRecord(recordPosition);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OClusterPage clusterPage = new OClusterPage(cacheEntry);
    clusterPage.appendRecord(recordVersion, record, recordPosition, Collections.emptySet());
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_PAGE_DELETE_RECORD_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE + record.length;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(recordPosition);
    buffer.putInt(recordVersion);

    buffer.putInt(record.length);
    buffer.put(record);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    recordPosition = buffer.getInt();
    recordVersion = buffer.getInt();

    final int recLen = buffer.getInt();
    record = new byte[recLen];
    buffer.get(record);
  }
}

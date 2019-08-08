package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

import java.nio.ByteBuffer;
import java.util.Collections;

public class ClusterPageAppendRecordPO extends PageOperationRecord {
  private int    recordVersion;
  private byte[] record;
  private int    requestedPosition;
  private int    recordPosition;

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

  public ClusterPageAppendRecordPO(int recordVersion, byte[] record, int requestedPosition, int recordPosition) {
    this.recordVersion = recordVersion;
    this.record = record;
    this.requestedPosition = requestedPosition;
    this.recordPosition = recordPosition;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OClusterPage clusterPage = new OClusterPage(cacheEntry);
    if (requestedPosition < 0) {
      clusterPage.appendRecord(recordVersion, record, recordPosition, Collections.emptySet());
    } else {
      clusterPage.appendRecord(recordVersion, record, requestedPosition, Collections.emptySet());
    }
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OClusterPage clusterPage = new OClusterPage(cacheEntry);
    clusterPage.deleteRecord(recordPosition);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_PAGE_APPEND_RECORD_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 4 * OIntegerSerializer.INT_SIZE + record.length;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(recordVersion);
    buffer.putInt(requestedPosition);
    buffer.putInt(recordPosition);

    buffer.putInt(record.length);
    buffer.put(record);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    recordVersion = buffer.getInt();
    requestedPosition = buffer.getInt();
    recordPosition = buffer.getInt();

    final int recordLen = buffer.getInt();
    record = new byte[recordLen];
    buffer.get(record);
  }
}

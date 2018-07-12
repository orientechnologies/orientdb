package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public class OClusterPageDeleteRecordOperation extends OPageOperation {
  private int    recordVersion;
  private byte[] record;

  public OClusterPageDeleteRecordOperation() {
  }

  public OClusterPageDeleteRecordOperation(OLogSequenceNumber lsn, long fileId, long pageIndex, int recordVersion, byte[] record) {
    super(lsn, fileId, pageIndex);
    this.recordVersion = recordVersion;
    this.record = record;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_PAGE_DELETER_RECORD_OPERATION;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE + record.length;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(recordVersion, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(record.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(record, 0, content, offset, record.length);
    offset += record.length;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(recordVersion);
    buffer.putInt(record.length);
    buffer.put(record);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    recordVersion = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    final int recordSize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    record = new byte[recordSize];
    System.arraycopy(content, offset, record, 0, record.length);
    offset += record.length;

    return offset;
  }

  public int getRecordVersion() {
    return recordVersion;
  }

  public byte[] getRecord() {
    return record;
  }
}

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;

public class OClusterPageReplaceRecordOperation extends OPageOperation {
  private int    oldRecordVersion;
  private byte[] oldRecord;

  public OClusterPageReplaceRecordOperation() {
  }

  public OClusterPageReplaceRecordOperation(OLogSequenceNumber lsn, long fileId, long pageIndex, int oldRecordVersion,
      byte[] oldRecord) {
    super(lsn, fileId, pageIndex);
    this.oldRecord = oldRecord;
    this.oldRecordVersion = oldRecordVersion;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_PAGE_REPLACE_RECORD_OPERATION;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE + oldRecord.length;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(oldRecordVersion, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(oldRecord.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(oldRecord, 0, content, offset, oldRecord.length);
    offset += oldRecord.length;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(oldRecordVersion);
    buffer.putInt(oldRecord.length);
    buffer.put(oldRecord);
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    oldRecordVersion = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    final int recordLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    oldRecord = new byte[recordLen];
    System.arraycopy(content, offset, oldRecord, 0, oldRecord.length);
    offset += oldRecord.length;

    return offset;
  }

  public int getOldRecordVersion() {
    return oldRecordVersion;
  }

  public byte[] getOldRecord() {
    return oldRecord;
  }
}

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import java.nio.ByteBuffer;

public class OFileTruncatedWALRecord extends OOperationUnitBodyRecord {
  private long fileId;

  public OFileTruncatedWALRecord() {}

  public OFileTruncatedWALRecord(long operationUnitId, long fileId) {
    super(operationUnitId);
    this.fileId = fileId;
  }

  public long getFileId() {
    return fileId;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    buffer.putLong(fileId);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    fileId = buffer.getLong();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OLongSerializer.LONG_SIZE;
  }

  @Override
  public int getId() {
    return WALRecordTypes.FILE_TRUNCATED_WAL_RECORD;
  }
}

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import java.nio.ByteBuffer;

public class OFileDeletedWALRecord extends OOperationUnitBodyRecord {
  private long fileId;

  public OFileDeletedWALRecord() {}

  public OFileDeletedWALRecord(long operationUnitId, long fileId) {
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
    return WALRecordTypes.FILE_DELETED_WAL_RECORD;
  }
}

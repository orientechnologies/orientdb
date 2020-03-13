package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;

import java.nio.ByteBuffer;

import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.HIGH_LEVEL_TRANSACTION_CHANGE_RECORD;

public class OHighLevelTransactionChangeRecord extends OOperationUnitRecordV2 {
  private byte[] data;

  public OHighLevelTransactionChangeRecord() {

  }

  public OHighLevelTransactionChangeRecord(Long operationUnitId, byte[] data) {
    super(operationUnitId);
    this.data = data;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);
    OIntegerSerializer.INSTANCE.serializeNative(data.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;
    System.arraycopy(data, 0, content, offset, data.length);
    offset += data.length;
    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);
    int size = OIntegerSerializer.INSTANCE.deserializeNative(data, offset);
    offset += OIntegerSerializer.INT_SIZE;
    this.data = new byte[size];
    System.arraycopy(content, offset, data, 0, size);
    offset += size;
    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + data.length;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public byte getId() {
    return HIGH_LEVEL_TRANSACTION_CHANGE_RECORD;
  }

  public byte[] getData() {
    return data;
  }
}

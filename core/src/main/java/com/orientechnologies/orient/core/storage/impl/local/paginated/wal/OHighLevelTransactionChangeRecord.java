package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.HIGH_LEVEL_TRANSACTION_CHANGE_RECORD;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import java.nio.ByteBuffer;

public class OHighLevelTransactionChangeRecord extends OOperationUnitRecord {
  private byte[] data;

  public OHighLevelTransactionChangeRecord() {}

  public OHighLevelTransactionChangeRecord(long operationUnitId, byte[] data) {
    super(operationUnitId);
    this.data = data;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    buffer.putInt(data.length);
    buffer.put(data, 0, data.length);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    int size = buffer.getInt();
    data = new byte[size];
    buffer.get(data, 0, size);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + data.length;
  }

  @Override
  public int getId() {
    return HIGH_LEVEL_TRANSACTION_CHANGE_RECORD;
  }

  public byte[] getData() {
    return data;
  }
}

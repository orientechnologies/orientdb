package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;

import java.nio.ByteBuffer;

public class OCreateRecordOperation extends OClusterOperation {
  private ORecordId rid;
  private byte[]    record;
  private int       recordVersion;
  private byte      recordType;

  public OCreateRecordOperation() {
  }

  public OCreateRecordOperation(int clusterId, OOperationUnitId operationUnitId, ORecordId rid, byte[] record, int recordVersion,
      byte recordType) {
    super(operationUnitId, clusterId);

    this.rid = rid;
    this.record = record;
    this.recordVersion = recordVersion;
    this.recordType = recordType;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OShortSerializer.INSTANCE.serializeNative((short) rid.getClusterId(), content, offset);
    offset += OShortSerializer.SHORT_SIZE;

    OLongSerializer.INSTANCE.serializeNative(rid.getClusterPosition(), content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(record.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(record, 0, content, offset, record.length);
    offset += record.length;

    OIntegerSerializer.INSTANCE.serializeNative(recordVersion, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    content[offset] = recordType;
    offset++;

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    final int clusterId = OShortSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OShortSerializer.SHORT_SIZE;

    final long position = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    rid = new ORecordId(clusterId, position);

    int recordLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    record = new byte[recordLen];
    System.arraycopy(content, offset, record, 0, recordLen);
    offset += recordLen;

    recordVersion = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    recordType = content[offset];
    offset++;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putShort((short) rid.getClusterId());
    buffer.putLong(rid.getClusterPosition());
    buffer.putInt(record.length);
    buffer.put(record);
    buffer.putInt(recordVersion);
    buffer.put(recordType);
  }

  @Override
  public int serializedSize() {
    int size = super.serializedSize();
    size += OShortSerializer.SHORT_SIZE;
    size += OLongSerializer.LONG_SIZE;
    size += OIntegerSerializer.INT_SIZE;
    size += record.length;
    size += OIntegerSerializer.INT_SIZE;
    size += OByteSerializer.BYTE_SIZE;

    return size;
  }
}

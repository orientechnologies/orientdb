package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;

import java.nio.ByteBuffer;
import java.util.Objects;

public class OAllocatePositionOperation extends OClusterOperation {
  private byte recordType;
  private long position;

  @SuppressWarnings("WeakerAccess")
  public OAllocatePositionOperation() {
  }

  public OAllocatePositionOperation(OOperationUnitId operationUnitId, int clusterId, long position, byte recordType) {
    super(operationUnitId, clusterId);
    this.recordType = recordType;
    this.position = position;
  }

  public byte getRecordType() {
    return recordType;
  }

  public long getPosition() {
    return position;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OLongSerializer.INSTANCE.serializeNative(position, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    content[offset] = recordType;
    offset++;

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    position = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    recordType = content[offset];
    offset++;
    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);
    buffer.putLong(position);
    buffer.put(recordType);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OLongSerializer.LONG_SIZE + OByteSerializer.BYTE_SIZE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;
    OAllocatePositionOperation that = (OAllocatePositionOperation) o;
    return recordType == that.recordType && position == that.position;
  }

  @Override
  public int hashCode() {

    return Objects.hash(super.hashCode(), recordType, position);
  }
}

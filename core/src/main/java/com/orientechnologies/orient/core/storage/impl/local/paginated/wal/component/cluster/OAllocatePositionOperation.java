package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;
import java.util.Objects;

public final class OAllocatePositionOperation extends OClusterOperation {
  private byte recordType;
  private long position;

  @SuppressWarnings("WeakerAccess")
  public OAllocatePositionOperation() {
  }

  public OAllocatePositionOperation(final OOperationUnitId operationUnitId, final int clusterId, final long position,
      final byte recordType) {
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
  public void rollbackOperation(final OPaginatedCluster cluster, final OAtomicOperation atomicOperation) {
    cluster.makePositionAvailableRollback(position, atomicOperation);
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OLongSerializer.INSTANCE.serializeNative(position, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    content[offset] = recordType;
    offset++;

    return offset;
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    position = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    recordType = content[offset];
    offset++;

    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);
    buffer.putLong(position);
    buffer.put(recordType);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OLongSerializer.LONG_SIZE + OByteSerializer.BYTE_SIZE;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.ALLOCATE_POSITION_OPERATION;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;
    final OAllocatePositionOperation that = (OAllocatePositionOperation) o;
    return recordType == that.recordType && position == that.position;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), recordType, position);
  }
}

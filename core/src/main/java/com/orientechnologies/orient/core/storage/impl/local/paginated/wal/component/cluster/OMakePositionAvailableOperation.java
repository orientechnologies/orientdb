package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;

import java.nio.ByteBuffer;
import java.util.Objects;

public class OMakePositionAvailableOperation extends OClusterOperation {
  private long position;

  public OMakePositionAvailableOperation() {
  }

  public OMakePositionAvailableOperation(OOperationUnitId operationUnitId, int clusterId, long position) {
    super(operationUnitId, clusterId);
    this.position = position;
  }

  public long getPosition() {
    return position;
  }

  @Override
  public void rollbackOperation(OPaginatedCluster cluster, OAtomicOperation atomicOperation) {
    throw new UnsupportedOperationException("This operation can not be rolled back");
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OLongSerializer.INSTANCE.serializeNative(position, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    position = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);
    buffer.putLong(position);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OLongSerializer.LONG_SIZE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;
    OMakePositionAvailableOperation that = (OMakePositionAvailableOperation) o;
    return position == that.position;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), position);
  }
}

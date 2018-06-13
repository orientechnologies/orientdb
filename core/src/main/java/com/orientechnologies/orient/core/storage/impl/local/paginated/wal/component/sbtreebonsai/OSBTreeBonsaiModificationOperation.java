package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;

import java.nio.ByteBuffer;
import java.util.Objects;

public abstract class OSBTreeBonsaiModificationOperation extends OSBTreeBonsaiOperation {
  private OBonsaiBucketPointer pointer;

  @SuppressWarnings("WeakerAccess")
  public OSBTreeBonsaiModificationOperation() {
  }

  OSBTreeBonsaiModificationOperation(OOperationUnitId operationUnitId, long fileId, OBonsaiBucketPointer pointer) {
    super(operationUnitId, fileId);
    this.pointer = pointer;
  }

  public OBonsaiBucketPointer getPointer() {
    return pointer;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);
    OIntegerSerializer.INSTANCE.serializeNative(pointer.getPageIndex(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(pointer.getVersion(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(pointer.getPageOffset(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    int pageIndex = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    int version = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    int pageOffset = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    pointer = new OBonsaiBucketPointer(pageIndex, pageOffset, version);

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(pointer.getPageIndex());
    buffer.putInt(pointer.getVersion());
    buffer.putInt(pointer.getPageOffset());
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;
    OSBTreeBonsaiModificationOperation that = (OSBTreeBonsaiModificationOperation) o;
    return Objects.equals(pointer, that.pointer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), pointer);
  }
}

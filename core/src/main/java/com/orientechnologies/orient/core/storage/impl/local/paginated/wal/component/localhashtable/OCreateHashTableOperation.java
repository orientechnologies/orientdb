package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.localhashtable;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;

import java.nio.ByteBuffer;
import java.util.Objects;

public class OCreateHashTableOperation extends OLocalHashTableOperation {
  private long fileId;
  private long directoryFileId;

  public OCreateHashTableOperation() {
  }

  public OCreateHashTableOperation(OOperationUnitId operationUnitId, String name, long fileId, long directoryFileId) {
    super(operationUnitId, name);
    this.fileId = fileId;
    this.directoryFileId = directoryFileId;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OLongSerializer.INSTANCE.serializeNative(fileId, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(directoryFileId, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    fileId = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    directoryFileId = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putLong(fileId);
    buffer.putLong(directoryFileId);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OLongSerializer.LONG_SIZE + OLongSerializer.LONG_SIZE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;
    OCreateHashTableOperation that = (OCreateHashTableOperation) o;
    return fileId == that.fileId && directoryFileId == that.directoryFileId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), fileId, directoryFileId);
  }
}

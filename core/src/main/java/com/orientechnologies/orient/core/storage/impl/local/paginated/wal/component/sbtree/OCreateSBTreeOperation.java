package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTree;

import java.nio.ByteBuffer;
import java.util.Objects;

public final class OCreateSBTreeOperation extends OSBTreeOperation {
  private long fileId;

  public OCreateSBTreeOperation() {
  }

  public OCreateSBTreeOperation(final OOperationUnitId operationUnitId, final String name, final long fileId) {
    super(operationUnitId, name);
    this.fileId = fileId;
  }

  public long getFileId() {
    return fileId;
  }

  @Override
  public void rollbackOperation(final OSBTree tree, final OAtomicOperation atomicOperation) {
    tree.deleteRollback(atomicOperation);
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OLongSerializer.INSTANCE.serializeNative(fileId, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    fileId = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putLong(fileId);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OLongSerializer.LONG_SIZE;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CREATE_SBTREE_OPERATION;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;
    final OCreateSBTreeOperation that = (OCreateSBTreeOperation) o;
    return fileId == that.fileId;
  }

  @Override
  public int hashCode() {

    return Objects.hash(super.hashCode(), fileId);
  }
}

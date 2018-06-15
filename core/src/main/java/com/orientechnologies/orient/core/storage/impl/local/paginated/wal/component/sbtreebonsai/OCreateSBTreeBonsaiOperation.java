package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai;

import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiLocal;

import java.nio.ByteBuffer;
import java.util.Objects;

public final class OCreateSBTreeBonsaiOperation extends OSBTreeBonsaiModificationOperation {
  private String name;

  public OCreateSBTreeBonsaiOperation() {
  }

  public OCreateSBTreeBonsaiOperation(final OOperationUnitId operationUnitId, final long fileId, final OBonsaiBucketPointer pointer,
      final String name) {
    super(operationUnitId, fileId, pointer);
    this.name = name;
  }


  public String getName() {
    return name;
  }

  @Override
  public void rollbackOperation(final OSBTreeBonsaiLocal tree, final OAtomicOperation atomicOperation) {
    tree.rollbackDelete(atomicOperation);
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);
    OStringSerializer.INSTANCE.serializeNativeObject(name, content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(name);

    return offset;
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);
    name = OStringSerializer.INSTANCE.deserializeNativeObject(content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(name);

    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    OStringSerializer.INSTANCE.serializeInByteBufferObject(name, buffer);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OStringSerializer.INSTANCE.getObjectSize(name);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CREATE_SBTREE_BONSAI_OPERATION;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;
    final OCreateSBTreeBonsaiOperation that = (OCreateSBTreeBonsaiOperation) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {

    return Objects.hash(super.hashCode(), name);
  }
}

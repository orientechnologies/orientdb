package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiLocal;

import java.nio.ByteBuffer;
import java.util.Arrays;

public final class OSBTreeBonsaiRemoveOperation extends OSBTreeBonsaiModificationOperation {
  private byte[] key;
  private byte[] value;

  @SuppressWarnings("unused")
  public OSBTreeBonsaiRemoveOperation() {
  }

  public OSBTreeBonsaiRemoveOperation(final OOperationUnitId operationUnitId, final long fileId, final OBonsaiBucketPointer pointer,
      final byte[] key, final byte[] value) {
    super(operationUnitId, fileId, pointer);
    this.key = key;
    this.value = value;
  }

  public byte[] getKey() {
    return key;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public void rollbackOperation(final OSBTreeBonsaiLocal tree, final OAtomicOperation atomicOperation) {
    tree.rollbackPut(key, value, atomicOperation);
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(key.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(key, 0, content, offset, key.length);
    offset += key.length;

    OIntegerSerializer.INSTANCE.serializeNative(value.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(value, 0, content, offset, value.length);
    offset += value.length;

    return offset;
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    final int keyLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    key = new byte[keyLen];
    System.arraycopy(content, offset, key, 0, keyLen);
    offset += keyLen;

    final int valueLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    value = new byte[valueLen];
    System.arraycopy(content, offset, value, 0, valueLen);
    offset += valueLen;

    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(key.length);
    buffer.put(key);
    buffer.putInt(value.length);
    buffer.put(value);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + key.length + OIntegerSerializer.INT_SIZE + value.length;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_REMOVE_OPERATION;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;
    final OSBTreeBonsaiRemoveOperation that = (OSBTreeBonsaiRemoveOperation) o;
    return Arrays.equals(key, that.key) && Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {

    int result = super.hashCode();
    result = 31 * result + Arrays.hashCode(key);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }
}

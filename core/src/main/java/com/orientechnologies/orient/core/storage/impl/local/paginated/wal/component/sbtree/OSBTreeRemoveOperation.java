package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTree;

import java.nio.ByteBuffer;
import java.util.Arrays;

public final class OSBTreeRemoveOperation extends OSBTreeOperation {
  private byte[] key;
  private byte[] oldValue;

  @SuppressWarnings("WeakerAccess")
  public OSBTreeRemoveOperation() {
  }

  public OSBTreeRemoveOperation(final OOperationUnitId operationUnitId, final String name, final byte[] key,
      final byte[] oldValue) {
    super(operationUnitId, name);
    this.key = key;
    this.oldValue = oldValue;
  }

  @Override
  public void rollbackOperation(final OSBTree tree, final OAtomicOperation atomicOperation) {
    tree.putRollback(key, oldValue, atomicOperation);
  }

  public byte[] getKey() {
    return key;
  }

  public byte[] getOldValue() {
    return oldValue;
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    if (key == null) {
      offset++;
    } else {
      content[offset] = 1;
      offset++;

      OIntegerSerializer.INSTANCE.serializeNative(key.length, content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      System.arraycopy(key, 0, content, offset, key.length);
      offset += key.length;
    }

    OIntegerSerializer.INSTANCE.serializeNative(oldValue.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(oldValue, 0, content, offset, oldValue.length);
    offset += oldValue.length;

    return offset;
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    if (content[offset] == 0) {
      offset++;
    } else {
      offset++;

      final int keyLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      key = new byte[keyLen];
      System.arraycopy(content, offset, key, 0, keyLen);
      offset += keyLen;
    }

    final int valueLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    oldValue = new byte[valueLen];
    System.arraycopy(content, offset, oldValue, 0, valueLen);
    offset += valueLen;

    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);
    if (key == null) {
      buffer.put((byte) 0);
    } else {
      buffer.put((byte) 1);

      buffer.putInt(key.length);
      buffer.put(key);
    }

    buffer.putInt(oldValue.length);
    buffer.put(oldValue);
  }

  @Override
  public int serializedSize() {
    int size = super.serializedSize();
    size += OByteSerializer.BYTE_SIZE;

    if (key != null) {
      size += OIntegerSerializer.INT_SIZE;
      size += key.length;
    }

    size += OIntegerSerializer.INT_SIZE;
    size += oldValue.length;

    return size;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_REMOVE_OPERATION;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;
    final OSBTreeRemoveOperation that = (OSBTreeRemoveOperation) o;
    return Arrays.equals(key, that.key) && Arrays.equals(oldValue, that.oldValue);
  }

  @Override
  public int hashCode() {

    int result = super.hashCode();
    result = 31 * result + Arrays.hashCode(key);
    result = 31 * result + Arrays.hashCode(oldValue);
    return result;
  }
}

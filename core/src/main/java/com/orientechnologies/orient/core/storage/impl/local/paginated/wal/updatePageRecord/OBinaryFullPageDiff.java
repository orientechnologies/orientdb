package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord;

import java.util.Arrays;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;

/**
 * @author Andrey Lomakin
 * @since 27.05.13
 */
public class OBinaryFullPageDiff extends OFullPageDiff<byte[]> {
  public OBinaryFullPageDiff() {
  }

  public OBinaryFullPageDiff(byte[] newValue, int pageOffset, byte[] oldValue) {
    super(newValue, pageOffset, oldValue);
  }

  @Override
  public void revertPageData(long pagePointer) {
    directMemory.set(pagePointer + pageOffset, oldValue, 0, oldValue.length);
  }

  @Override
  public void restorePageData(long pagePointer) {
    directMemory.set(pagePointer + pageOffset, newValue, 0, newValue.length);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE + newValue.length + oldValue.length;
  }

  @Override
  public int toStream(byte[] stream, int offset) {
    offset = super.toStream(stream, offset);

    OIntegerSerializer.INSTANCE.serializeNative(oldValue.length, stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(oldValue, 0, stream, offset, oldValue.length);
    offset += oldValue.length;

    OIntegerSerializer.INSTANCE.serializeNative(newValue.length, stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(newValue, 0, stream, offset, newValue.length);
    offset += newValue.length;

    return offset;
  }

  @Override
  public int fromStream(byte[] stream, int offset) {
    offset = super.fromStream(stream, offset);

    int oldValueLen = OIntegerSerializer.INSTANCE.deserializeNative(stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    oldValue = new byte[oldValueLen];
    System.arraycopy(stream, offset, oldValue, 0, oldValueLen);
    offset += oldValueLen;

    int newValueLen = OIntegerSerializer.INSTANCE.deserializeNative(stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    newValue = new byte[newValueLen];
    System.arraycopy(stream, offset, newValue, 0, newValueLen);
    offset += newValueLen;

    return offset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OBinaryFullPageDiff diff = (OBinaryFullPageDiff) o;

    if (pageOffset != diff.pageOffset)
      return false;
    if (!Arrays.equals(newValue, diff.newValue))
      return false;
    if (!Arrays.equals(oldValue, diff.oldValue))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(newValue);
    result = 31 * result + Arrays.hashCode(oldValue);
    result = 31 * result + pageOffset;
    return result;
  }

}

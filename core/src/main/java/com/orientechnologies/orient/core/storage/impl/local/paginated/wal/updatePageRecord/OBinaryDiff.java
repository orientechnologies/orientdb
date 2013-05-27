package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord;

import java.util.Arrays;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;

/**
 * @author Andrey Lomakin
 * @since 27.05.13
 */
public class OBinaryDiff extends OPageDiff<byte[]> {
  public OBinaryDiff() {
  }

  public OBinaryDiff(byte[] newValue, int pageOffset) {
    super(newValue, pageOffset);
  }

  @Override
  public void restorePageData(long pagePointer) {
    directMemory.set(pagePointer + pageOffset, newValue, 0, newValue.length);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + newValue.length;
  }

  @Override
  public int toStream(byte[] stream, int offset) {
    offset = super.toStream(stream, offset);
    OIntegerSerializer.INSTANCE.serializeNative(newValue.length, stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(newValue, 0, stream, offset, newValue.length);
    offset += newValue.length;

    return offset;
  }

  @Override
  public int fromStream(byte[] stream, int offset) {
    offset = super.fromStream(stream, offset);

    int len = OIntegerSerializer.INSTANCE.deserializeNative(stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    newValue = new byte[len];
    System.arraycopy(stream, offset, newValue, 0, len);
    offset += len;

    return offset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OBinaryDiff diff = (OBinaryDiff) o;

    if (pageOffset != diff.pageOffset)
      return false;
    if (!Arrays.equals(newValue, diff.newValue))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(newValue);
    result = 31 * result + pageOffset;
    return result;
  }
}

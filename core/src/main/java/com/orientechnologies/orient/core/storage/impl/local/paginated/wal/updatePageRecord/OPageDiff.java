package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;

/**
 * @author Andrey Lomakin
 * @since 27.05.13
 */
public abstract class OPageDiff<T> {
  protected final ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

  protected T                   newValue;

  protected int                 pageOffset;

  OPageDiff() {
  }

  public OPageDiff(T newValue, int pageOffset) {
    this.newValue = newValue;
    this.pageOffset = pageOffset;
  }

  public T getNewValue() {
    return newValue;
  }

  public int serializedSize() {
    return OIntegerSerializer.INT_SIZE;
  }

  public int toStream(byte[] stream, int offset) {
    OIntegerSerializer.INSTANCE.serializeNative(pageOffset, stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  public int fromStream(byte[] stream, int offset) {
    pageOffset = OIntegerSerializer.INSTANCE.deserializeNative(stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  public int getPageOffset() {
    return pageOffset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OPageDiff oPageDiff = (OPageDiff) o;

    if (pageOffset != oPageDiff.pageOffset)
      return false;
    if (!newValue.equals(oPageDiff.newValue))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = newValue.hashCode();
    result = 31 * result + pageOffset;
    return result;
  }

  public abstract void restorePageData(long pagePointer);
}

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;

/**
 * @author Andrey Lomakin
 * @since 27.05.13
 */
public class OIntDiff extends OPageDiff<Integer> {
  public OIntDiff() {
  }

  public OIntDiff(Integer newValue, int pageOffset) {
    super(newValue, pageOffset);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int toStream(byte[] stream, int offset) {
    offset = super.toStream(stream, offset);
    OIntegerSerializer.INSTANCE.serializeNative(newValue, stream, offset);

    return offset + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public int fromStream(byte[] stream, int offset) {
    offset = super.fromStream(stream, offset);
    newValue = OIntegerSerializer.INSTANCE.deserializeNative(stream, offset);
    return offset + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public void restorePageData(long pagePointer) {
    OIntegerSerializer.INSTANCE.serializeInDirectMemory(newValue, directMemory, pagePointer + pageOffset);
  }
}

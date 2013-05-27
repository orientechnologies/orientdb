package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord;

import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 27.05.13
 */
public class OLongDiff extends OPageDiff<Long> {
  public OLongDiff(Long newValue, int pageOffset) {
    super(newValue, pageOffset);
  }

  public OLongDiff() {
  }

  @Override
  public void restorePageData(long pagePointer) {
    OLongSerializer.INSTANCE.serializeInDirectMemory(newValue, directMemory, pagePointer + pageOffset);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OLongSerializer.LONG_SIZE;
  }

  @Override
  public int toStream(byte[] stream, int offset) {
    offset = super.toStream(stream, offset);
    OLongSerializer.INSTANCE.serializeNative(newValue, stream, offset);

    return offset + OLongSerializer.LONG_SIZE;
  }

  @Override
  public int fromStream(byte[] stream, int offset) {
    offset = super.fromStream(stream, offset);
    newValue = OLongSerializer.INSTANCE.deserializeNative(stream, offset);

    return offset + OLongSerializer.LONG_SIZE;
  }
}

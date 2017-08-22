package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;

import java.nio.ByteBuffer;

public class OWALPageV1 implements OWALPage {
  static final long MAGIC_NUMBER = 0xFACB03FEL;

  static final int RECORDS_OFFSET = FREE_SPACE_OFFSET + OIntegerSerializer.INT_SIZE;

  static final int MAX_ENTRY_SIZE = PAGE_SIZE - RECORDS_OFFSET;

  private final ByteBuffer buffer;

  OWALPageV1(ByteBuffer buffer, boolean isNew) {
    this.buffer = buffer;

    if (isNew) {
      buffer.position(MAGIC_NUMBER_OFFSET);

      buffer.putLong(MAGIC_NUMBER);
      buffer.putInt(MAX_ENTRY_SIZE);
    }
  }

  public byte[] getRecord(int position) {
    buffer.position(position + 2);
    final int recordSize = buffer.getInt();
    final byte[] record = new byte[recordSize];
    buffer.get(record);
    return record;
  }

  public boolean mergeWithNextPage(int position) {
    return buffer.get(position) > 0;
  }

  public boolean isEmpty() {
    return getFreeSpace() == MAX_ENTRY_SIZE;
  }

  public int getFreeSpace() {
    return buffer.getInt(FREE_SPACE_OFFSET);
  }

  public int getFilledUpTo() {
    return OWALPage.PAGE_SIZE - getFreeSpace();
  }

  static int calculateSerializedSize(int recordSize) {
    return recordSize + OIntegerSerializer.INT_SIZE + 2;
  }

  static int calculateRecordSize(int serializedSize) {
    return serializedSize - OIntegerSerializer.INT_SIZE - 2;
  }
}

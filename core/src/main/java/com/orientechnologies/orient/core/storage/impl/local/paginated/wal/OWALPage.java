package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;

/**
 * @author Andrey Lomakin
 * @since 5/8/13
 */
public class OWALPage {
  public static final int     PAGE_SIZE         = 65536;
  public static final int     MIN_RECORD_SIZE   = OIntegerSerializer.INT_SIZE + 3;

  public static final int     CRC_OFFSET        = 0;
  private static final int    FREE_SPACE_OFFSET = CRC_OFFSET + OIntegerSerializer.INT_SIZE;
  public static final int     RECORDS_OFFSET    = FREE_SPACE_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final int    MAX_ENTRY_SIZE    = PAGE_SIZE - RECORDS_OFFSET;

  private final long          pagePointer;

  private final ODirectMemory directMemory      = ODirectMemoryFactory.INSTANCE.directMemory();

  public OWALPage(long pagePointer, boolean isNew) {
    this.pagePointer = pagePointer;

    if (isNew)
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(MAX_ENTRY_SIZE, directMemory, pagePointer + FREE_SPACE_OFFSET);
  }

  public long getPagePointer() {
    return pagePointer;
  }

  public int appendRecord(byte[] content, boolean mergeWithNextPage, boolean recordTail) {
    int freeSpace = getFreeSpace();
    int freePosition = PAGE_SIZE - freeSpace;
    int position = freePosition;

    directMemory.setByte(pagePointer + position, mergeWithNextPage ? (byte) 1 : 0);
    position++;

    directMemory.setByte(pagePointer + position, recordTail ? (byte) 1 : 0);
    position++;

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(content.length, directMemory, pagePointer + position);
    position += OIntegerSerializer.INT_SIZE;

    directMemory.set(pagePointer + position, content, 0, content.length);
    position += content.length;

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(freeSpace - 2 - OIntegerSerializer.INT_SIZE - content.length, directMemory,
        pagePointer + FREE_SPACE_OFFSET);

    return freePosition;
  }

  public byte[] getRecord(int position) {
    position += 2;
    int recordSize = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + position);
    position += OIntegerSerializer.INT_SIZE;

    return directMemory.get(pagePointer + position, recordSize);
  }

  public int getSerializedRecordSize(int position) {
    int recordSize = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + position + 2);
    return recordSize + OIntegerSerializer.INT_SIZE + 2;
  }

  public boolean mergeWithNextPage(int position) {
    return directMemory.getByte(pagePointer + position) > 0;
  }

  public boolean recordTail(int position) {
    return directMemory.getByte(pagePointer + position + 1) > 0;
  }

  public boolean isEmpty() {
    return getFreeSpace() == MAX_ENTRY_SIZE;
  }

  public int getFreeSpace() {
    return OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + FREE_SPACE_OFFSET);
  }

  public int getFilledUpTo() {
    return OWALPage.PAGE_SIZE - getFreeSpace();
  }

  public static int calculateSerializedSize(int recordSize) {
    return recordSize + OIntegerSerializer.INT_SIZE + 2;
  }

  public static int calculateRecordSize(int serializedSize) {
    return serializedSize - OIntegerSerializer.INT_SIZE - 2;
  }

  public void truncateTill(int pageOffset) {
    OIntegerSerializer.INSTANCE.serializeInDirectMemory(OWALPage.PAGE_SIZE - pageOffset, directMemory, pagePointer
        + FREE_SPACE_OFFSET);
  }
}

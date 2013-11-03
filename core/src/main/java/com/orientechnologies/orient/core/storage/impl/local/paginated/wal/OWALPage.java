package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 5/8/13
 */
public class OWALPage {
  public static final int            PAGE_SIZE          = 65536;
  public static final int            MIN_RECORD_SIZE    = OIntegerSerializer.INT_SIZE + 3;

  public static final int            CRC_OFFSET         = 0;
  public static final int            FLUSH_ID_OFFSET    = CRC_OFFSET + OIntegerSerializer.INT_SIZE;
  public static final int            FLUSH_INDEX_OFFSET = FLUSH_ID_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int           FREE_SPACE_OFFSET  = FLUSH_INDEX_OFFSET + OIntegerSerializer.INT_SIZE;
  public static final int            RECORDS_OFFSET     = FREE_SPACE_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final int           MAX_ENTRY_SIZE     = PAGE_SIZE - RECORDS_OFFSET;

  private final ODirectMemoryPointer pagePointer;

  public OWALPage(ODirectMemoryPointer pagePointer, boolean isNew) {
    this.pagePointer = pagePointer;

    if (isNew)
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(MAX_ENTRY_SIZE, pagePointer, FREE_SPACE_OFFSET);
  }

  public ODirectMemoryPointer getPagePointer() {
    return pagePointer;
  }

  public int appendRecord(byte[] content, boolean mergeWithNextPage, boolean recordTail) {
    int freeSpace = getFreeSpace();
    int freePosition = PAGE_SIZE - freeSpace;
    int position = freePosition;

    pagePointer.setByte(position, mergeWithNextPage ? (byte) 1 : 0);
    position++;

    pagePointer.setByte(position, recordTail ? (byte) 1 : 0);
    position++;

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(content.length, pagePointer, position);
    position += OIntegerSerializer.INT_SIZE;

    pagePointer.set(position, content, 0, content.length);
    position += content.length;

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(freeSpace - 2 - OIntegerSerializer.INT_SIZE - content.length, pagePointer,
        FREE_SPACE_OFFSET);

    return freePosition;
  }

  public byte[] getRecord(int position) {
    position += 2;
    int recordSize = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(pagePointer, position);
    position += OIntegerSerializer.INT_SIZE;

    return pagePointer.get(position, recordSize);
  }

  public int getSerializedRecordSize(int position) {
    int recordSize = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(pagePointer, position + 2);
    return recordSize + OIntegerSerializer.INT_SIZE + 2;
  }

  public boolean mergeWithNextPage(int position) {
    return pagePointer.getByte(position) > 0;
  }

  public boolean recordTail(int position) {
    return pagePointer.getByte(position + 1) > 0;
  }

  public boolean isEmpty() {
    return getFreeSpace() == MAX_ENTRY_SIZE;
  }

  public int getFreeSpace() {
    return OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(pagePointer, FREE_SPACE_OFFSET);
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
    OIntegerSerializer.INSTANCE.serializeInDirectMemory(OWALPage.PAGE_SIZE - pageOffset, pagePointer, FREE_SPACE_OFFSET);
  }
}

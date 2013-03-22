/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

/**
 * @author Andrey Lomakin
 * @since 19.03.13
 */
public class OLocalPage {
  private static final int    FREELIST_HEADER            = 0;
  private static final int    FREE_POSITION_OFFSET       = FREELIST_HEADER + OIntegerSerializer.INT_SIZE;
  private static final int    FREE_SPACE_COUNTER_OFFSET  = FREE_POSITION_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int    RECORDS_COUNT_OFFSET       = FREE_SPACE_COUNTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int    PAGE_INDEXES_LENGTH_OFFSET = RECORDS_COUNT_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int    PAGE_INDEXES_OFFSET        = PAGE_INDEXES_LENGTH_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final int    POSITION_MASK              = 0xFFFF;
  private static final int    MARKED_AS_DELETED_FLAG     = 1;
  private static final int    CLEANED_OUT_FLAG           = 3;

  public static final int     PAGE_SIZE                  = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger();

  private final long          pagePointer;
  private final ODirectMemory directMemory               = ODirectMemoryFactory.INSTANCE.directMemory();

  public OLocalPage(long pagePointer, boolean newPage) {
    this.pagePointer = pagePointer;

    if (newPage) {
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(PAGE_SIZE, directMemory, pagePointer + FREE_POSITION_OFFSET);
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(PAGE_SIZE - PAGE_INDEXES_OFFSET, directMemory, pagePointer
          + FREE_SPACE_COUNTER_OFFSET);
    }
  }

  public int appendRecord(byte[] record) {
    int freePosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + FREE_POSITION_OFFSET);
    int indexesLength = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, PAGE_INDEXES_LENGTH_OFFSET
        + pagePointer);

    int lastRecordIndexPosition = PAGE_INDEXES_OFFSET + indexesLength * OIntegerSerializer.INT_SIZE;
    int entrySize = record.length + OIntegerSerializer.INT_SIZE;
    int freeListHeader = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + FREELIST_HEADER);
    if (freeListHeader > 0) {
      if (freePosition - entrySize < lastRecordIndexPosition)
        return -1;
    } else {
      if (freePosition - entrySize < lastRecordIndexPosition + OIntegerSerializer.INT_SIZE)
        return -1;
    }

    freePosition -= entrySize;

    long entryPointer = pagePointer + freePosition;
    OIntegerSerializer.INSTANCE.serializeInDirectMemory(entrySize, directMemory, entryPointer);
    entryPointer += OIntegerSerializer.INT_SIZE;

    directMemory.set(entryPointer, record, record.length);
    entryPointer += record.length;

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(freePosition, directMemory, pagePointer + FREE_POSITION_OFFSET);

    int entryIndex;

    if (freeListHeader > 0) {
      entryIndex = freeListHeader - 1;

      final int tombstonePointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer
          + PAGE_INDEXES_OFFSET + OIntegerSerializer.INT_SIZE * entryIndex);

      int nextEntryPosition = tombstonePointer & POSITION_MASK;
      if (nextEntryPosition > 0)
        OIntegerSerializer.INSTANCE.serializeInDirectMemory(nextEntryPosition, directMemory, pagePointer + FREELIST_HEADER);
      else
        OIntegerSerializer.INSTANCE.serializeInDirectMemory(0, directMemory, pagePointer + FREELIST_HEADER);
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(getFreeSpace() - entrySize, directMemory, pagePointer
          + FREE_SPACE_COUNTER_OFFSET);
    } else {
      entryIndex = indexesLength;
      OIntegerSerializer.INSTANCE
          .serializeInDirectMemory(indexesLength + 1, directMemory, pagePointer + PAGE_INDEXES_LENGTH_OFFSET);
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(getFreeSpace() - entrySize - OIntegerSerializer.INT_SIZE, directMemory,
          pagePointer + FREE_SPACE_COUNTER_OFFSET);
    }

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(getRecordsCount() + 1, directMemory, pagePointer + RECORDS_COUNT_OFFSET);
    OIntegerSerializer.INSTANCE.serializeInDirectMemory(freePosition, directMemory, pagePointer + PAGE_INDEXES_OFFSET + entryIndex
        * OIntegerSerializer.INT_SIZE);

    return entryIndex;
  }

  public boolean markRecordAsDeleted(int position) {
    int indexesLength = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer
        + PAGE_INDEXES_LENGTH_OFFSET);
    if (position >= indexesLength)
      return false;

    int entryIndexPosition = PAGE_INDEXES_OFFSET + OIntegerSerializer.INT_SIZE * position;
    int entryPointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + entryIndexPosition);

    int entryFlag = entryPointer >>> 16;
    if (entryFlag != 0)
      return false;

    int entryPosition = entryPointer & POSITION_MASK;
    int entrySize = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + entryPosition);
    OIntegerSerializer.INSTANCE.serializeInDirectMemory(getFreeSpace() + entrySize, directMemory, pagePointer
        + FREE_SPACE_COUNTER_OFFSET);

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(entryPosition | (MARKED_AS_DELETED_FLAG << 16), directMemory, pagePointer
        + entryIndexPosition);
    return true;
  }

  public boolean isDeleted(int position) {
    int entryIndexPosition = PAGE_INDEXES_OFFSET + OIntegerSerializer.INT_SIZE * position;
    int entryPointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + entryIndexPosition);

    int entryFlag = entryPointer >>> 16;
    return entryFlag == MARKED_AS_DELETED_FLAG;
  }

  public boolean isCleanedOut(int position) {
    int entryIndexPosition = PAGE_INDEXES_OFFSET + OIntegerSerializer.INT_SIZE * position;
    int entryPointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + entryIndexPosition);

    int entryFlag = entryPointer >>> 16;
    return entryFlag == CLEANED_OUT_FLAG;
  }

  public long getRecordPointer(int position) {
    int entryIndexPosition = PAGE_INDEXES_OFFSET + OIntegerSerializer.INT_SIZE * position;
    int entryPointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + entryIndexPosition);
    int entryFlag = entryPointer >>> 16;

    int entryPosition = entryPointer & POSITION_MASK;
    if (entryFlag == CLEANED_OUT_FLAG)
      return ODirectMemory.NULL_POINTER;

    return pagePointer + entryPosition + OIntegerSerializer.INT_SIZE;
  }

  public int findFirstDeletedRecord(int position) {
    int indexesLength = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer
        + PAGE_INDEXES_LENGTH_OFFSET);
    for (int i = position; i < indexesLength; i++) {
      int entryIndexPosition = PAGE_INDEXES_OFFSET + OIntegerSerializer.INT_SIZE * i;
      int entryPointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + entryIndexPosition);
      int entryFlag = entryPointer >>> 16;

      if (entryFlag == MARKED_AS_DELETED_FLAG)
        return i;
    }

    return -1;
  }

  public void cleanOutRecord(int position) {
    int entryIndexPosition = PAGE_INDEXES_OFFSET + OIntegerSerializer.INT_SIZE * position;
    int entryPointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + entryIndexPosition);

    int entryFlag = entryPointer >>> 16;
    int entryPosition = entryPointer & POSITION_MASK;
    if (entryFlag == CLEANED_OUT_FLAG)
      return;

    int freeListHeader = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + FREELIST_HEADER);
    if (freeListHeader <= 0)
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(CLEANED_OUT_FLAG << 16, directMemory, pagePointer + entryIndexPosition);
    else
      OIntegerSerializer.INSTANCE.serializeInDirectMemory((CLEANED_OUT_FLAG << 16) | freeListHeader, directMemory, pagePointer
          + entryIndexPosition);

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(position + 1, directMemory, pagePointer + FREELIST_HEADER);

    int entrySize = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + entryPosition);

    int freePosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + FREE_POSITION_OFFSET);

    if (entryPosition > freePosition)
      directMemory.copyData(pagePointer + freePosition, pagePointer + freePosition + entrySize, entryPosition - freePosition);

    int indexesLength = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer
        + PAGE_INDEXES_LENGTH_OFFSET);
    for (int i = 0; i < indexesLength; i++) {
      int currentEntryIndexPosition = PAGE_INDEXES_OFFSET + OIntegerSerializer.INT_SIZE * i;
      int currentEntryPointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer
          + currentEntryIndexPosition);

      int currentEntryFlag = currentEntryPointer >>> 16;
      int currentEntryPosition = currentEntryPointer & POSITION_MASK;
      if (currentEntryPosition < entryPosition && (currentEntryFlag == 0 || currentEntryFlag == MARKED_AS_DELETED_FLAG)) {
        currentEntryPosition += entrySize;
        OIntegerSerializer.INSTANCE.serializeInDirectMemory(currentEntryPosition | currentEntryFlag << 16, directMemory,
            pagePointer + currentEntryIndexPosition);
      }
    }

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(getRecordsCount() - 1, directMemory, pagePointer + RECORDS_COUNT_OFFSET);
    if (entryFlag == 0)
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(getFreeSpace() + entrySize, directMemory, pagePointer
          + FREE_SPACE_COUNTER_OFFSET);

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(freePosition + entrySize, directMemory, pagePointer + FREE_POSITION_OFFSET);
  }

  public int getFreeSpace() {
    return OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + FREE_SPACE_COUNTER_OFFSET);
  }

  public int getRecordsCount() {
    return OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + RECORDS_COUNT_OFFSET);
  }
}

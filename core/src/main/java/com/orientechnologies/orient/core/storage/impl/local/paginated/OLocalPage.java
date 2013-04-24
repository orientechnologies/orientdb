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

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;

/**
 * @author Andrey Lomakin
 * @since 19.03.13
 */
public class OLocalPage {
  private static final int    VERSION_SIZE               = OVersionFactory.instance().getVersionSize();

  private static final int    MAGIC_NUMBER_OFFSET        = 0;
  private static final int    CRC32_OFFSET               = MAGIC_NUMBER_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int    NEXT_PAGE_OFFSET           = CRC32_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int    PREV_PAGE_OFFSET           = NEXT_PAGE_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int    FREELIST_HEADER_OFFSET     = PREV_PAGE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int    FREE_POSITION_OFFSET       = FREELIST_HEADER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int    FREE_SPACE_COUNTER_OFFSET  = FREE_POSITION_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int    ENTRIES_COUNT_OFFSET       = FREE_SPACE_COUNTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int    PAGE_INDEXES_LENGTH_OFFSET = ENTRIES_COUNT_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int    PAGE_INDEXES_OFFSET        = PAGE_INDEXES_LENGTH_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final int    INDEX_ITEM_SIZE            = OIntegerSerializer.INT_SIZE + VERSION_SIZE;
  private static final int    MARKED_AS_DELETED_FLAG     = 1 << 16;
  private static final int    POSITION_MASK              = 0xFFFF;
  public static final int     PAGE_SIZE                  = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  public static final int     MAX_ENTRY_SIZE             = PAGE_SIZE - PAGE_INDEXES_OFFSET - INDEX_ITEM_SIZE;

  public static final int     MAX_RECORD_SIZE            = MAX_ENTRY_SIZE - 2 * OIntegerSerializer.INT_SIZE;

  private final long          pagePointer;
  private final ODirectMemory directMemory               = ODirectMemoryFactory.INSTANCE.directMemory();

  public OLocalPage(long pagePointer, boolean newPage) {
    this.pagePointer = pagePointer;

    if (newPage) {
      OLongSerializer.INSTANCE.serializeInDirectMemory(-1L, directMemory, pagePointer + NEXT_PAGE_OFFSET);
      OLongSerializer.INSTANCE.serializeInDirectMemory(-1L, directMemory, pagePointer + PREV_PAGE_OFFSET);

      OIntegerSerializer.INSTANCE.serializeInDirectMemory(PAGE_SIZE, directMemory, pagePointer + FREE_POSITION_OFFSET);
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(PAGE_SIZE - PAGE_INDEXES_OFFSET, directMemory, pagePointer
          + FREE_SPACE_COUNTER_OFFSET);
    }
  }

  public int appendRecord(ORecordVersion recordVersion, byte[] record) {
    int freePosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + FREE_POSITION_OFFSET);
    int indexesLength = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer
        + PAGE_INDEXES_LENGTH_OFFSET);

    int lastEntryIndexPosition = PAGE_INDEXES_OFFSET + indexesLength * INDEX_ITEM_SIZE;

    int entrySize = record.length + 2 * OIntegerSerializer.INT_SIZE;
    int freeListHeader = OIntegerSerializer.INSTANCE
        .deserializeFromDirectMemory(directMemory, pagePointer + FREELIST_HEADER_OFFSET);

    if (!checkSpace(entrySize, freeListHeader))
      return -1;

    if (freeListHeader > 0) {
      if (freePosition - entrySize < lastEntryIndexPosition)
        doDefragmentation();
    } else {
      if (freePosition - entrySize < lastEntryIndexPosition + INDEX_ITEM_SIZE)
        doDefragmentation();
    }

    freePosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + FREE_POSITION_OFFSET);
    freePosition -= entrySize;
    int entryIndex;

    if (freeListHeader > 0) {
      entryIndex = freeListHeader - 1;

      final int tombstonePointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer
          + PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * entryIndex);

      int nextEntryPosition = tombstonePointer & POSITION_MASK;
      if (nextEntryPosition > 0)
        OIntegerSerializer.INSTANCE.serializeInDirectMemory(nextEntryPosition, directMemory, pagePointer + FREELIST_HEADER_OFFSET);
      else
        OIntegerSerializer.INSTANCE.serializeInDirectMemory(0, directMemory, pagePointer + FREELIST_HEADER_OFFSET);

      OIntegerSerializer.INSTANCE.serializeInDirectMemory(getFreeSpace() - entrySize, directMemory, pagePointer
          + FREE_SPACE_COUNTER_OFFSET);

      int entryIndexPosition = PAGE_INDEXES_OFFSET + entryIndex * INDEX_ITEM_SIZE;
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(freePosition, directMemory, pagePointer + entryIndexPosition);

      byte[] serializedVersion = directMemory.get(pagePointer + entryIndexPosition + OIntegerSerializer.INT_SIZE, OVersionFactory
          .instance().getVersionSize());
      ORecordVersion existingRecordVersion = OVersionFactory.instance().createVersion();
      existingRecordVersion.getSerializer().fastReadFrom(serializedVersion, 0, existingRecordVersion);

      if (existingRecordVersion.compareTo(recordVersion) < 0) {
        recordVersion.getSerializer().fastWriteTo(serializedVersion, 0, recordVersion);
        directMemory.set(pagePointer + entryIndexPosition + OIntegerSerializer.INT_SIZE, serializedVersion,
            serializedVersion.length);
      } else {
        existingRecordVersion.increment();
        existingRecordVersion.getSerializer().fastWriteTo(serializedVersion, 0, existingRecordVersion);
        directMemory.set(pagePointer + entryIndexPosition + OIntegerSerializer.INT_SIZE, serializedVersion,
            serializedVersion.length);
      }
    } else {
      entryIndex = indexesLength;
      OIntegerSerializer.INSTANCE
          .serializeInDirectMemory(indexesLength + 1, directMemory, pagePointer + PAGE_INDEXES_LENGTH_OFFSET);
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(getFreeSpace() - entrySize - INDEX_ITEM_SIZE, directMemory, pagePointer
          + FREE_SPACE_COUNTER_OFFSET);

      int entryIndexPosition = PAGE_INDEXES_OFFSET + entryIndex * INDEX_ITEM_SIZE;
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(freePosition, directMemory, pagePointer + entryIndexPosition);

      byte[] serializedVersion = new byte[OVersionFactory.instance().getVersionSize()];
      recordVersion.getSerializer().fastWriteTo(serializedVersion, 0, recordVersion);
      directMemory.set(pagePointer + entryIndexPosition + OIntegerSerializer.INT_SIZE, serializedVersion, serializedVersion.length);
    }

    long entryPointer = pagePointer + freePosition;
    OIntegerSerializer.INSTANCE.serializeInDirectMemory(entrySize, directMemory, entryPointer);
    entryPointer += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(entryIndex, directMemory, entryPointer);
    entryPointer += OIntegerSerializer.INT_SIZE;

    directMemory.set(entryPointer, record, record.length);
    entryPointer += record.length;

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(freePosition, directMemory, pagePointer + FREE_POSITION_OFFSET);

    incrementEntriesCount();

    return entryIndex;
  }

  public ORecordVersion getRecordVersion(int position) {
    int entryIndexPosition = PAGE_INDEXES_OFFSET + position * INDEX_ITEM_SIZE;
    byte[] serializedVersion = directMemory.get(pagePointer + entryIndexPosition + OIntegerSerializer.INT_SIZE, OVersionFactory
        .instance().getVersionSize());
    ORecordVersion recordVersion = OVersionFactory.instance().createVersion();
    recordVersion.getSerializer().fastReadFrom(serializedVersion, 0, recordVersion);

    return recordVersion;
  }

  public boolean isEmpty() {
    return getFreeSpace() == PAGE_SIZE - PAGE_INDEXES_OFFSET;
  }

  private boolean checkSpace(int entrySize, int freeListHeader) {
    if (freeListHeader > 0) {
      if (getFreeSpace() - entrySize < 0)
        return false;
    } else {
      if (getFreeSpace() - entrySize - INDEX_ITEM_SIZE < 0)
        return false;
    }
    return true;
  }

  public boolean deleteRecord(int position) {
    int indexesLength = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer
        + PAGE_INDEXES_LENGTH_OFFSET);
    if (position >= indexesLength)
      return false;

    int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * position;
    int entryPointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + entryIndexPosition);

    if ((entryPointer & MARKED_AS_DELETED_FLAG) > 0)
      return false;

    int entryPosition = entryPointer & POSITION_MASK;

    int freeListHeader = OIntegerSerializer.INSTANCE
        .deserializeFromDirectMemory(directMemory, pagePointer + FREELIST_HEADER_OFFSET);
    if (freeListHeader <= 0)
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(MARKED_AS_DELETED_FLAG, directMemory, pagePointer + entryIndexPosition);
    else
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(freeListHeader | MARKED_AS_DELETED_FLAG, directMemory, pagePointer
          + entryIndexPosition);

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(position + 1, directMemory, pagePointer + FREELIST_HEADER_OFFSET);

    final int entrySize = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + entryPosition);
    assert entrySize > 0;

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(-entrySize, directMemory, pagePointer + entryPosition);

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(getFreeSpace() + entrySize, directMemory, pagePointer
        + FREE_SPACE_COUNTER_OFFSET);

    decrementEntriesCount();

    return true;
  }

  public boolean isDeleted(int position) {
    int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * position;
    int entryPointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + entryIndexPosition);

    return (entryPointer & MARKED_AS_DELETED_FLAG) > 0;
  }

  public long getRecordPointer(int position) {
    int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * position;
    int entryPointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + entryIndexPosition);
    if ((entryPointer & MARKED_AS_DELETED_FLAG) > 0)
      return ODirectMemory.NULL_POINTER;

    int entryPosition = entryPointer & POSITION_MASK;
    return pagePointer + entryPosition + 2 * OIntegerSerializer.INT_SIZE;
  }

  public int getRecordSize(int position) {
    int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * position;
    int entryPointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + entryIndexPosition);
    if ((entryPointer & MARKED_AS_DELETED_FLAG) > 0)
      return -1;

    int entryPosition = entryPointer & POSITION_MASK;
    return OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + entryPosition) - 2
        * OIntegerSerializer.INT_SIZE;
  }

  public int findFirstDeletedRecord(int position) {
    int indexesLength = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer
        + PAGE_INDEXES_LENGTH_OFFSET);
    for (int i = position; i < indexesLength; i++) {
      int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * i;
      int entryPointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + entryIndexPosition);
      if ((entryPointer & MARKED_AS_DELETED_FLAG) > 0)
        return i;
    }

    return -1;
  }

  public int findFirstRecord(int position) {
    int indexesLength = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer
        + PAGE_INDEXES_LENGTH_OFFSET);
    for (int i = position; i < indexesLength; i++) {
      int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * i;
      int entryPointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + entryIndexPosition);
      if ((entryPointer & MARKED_AS_DELETED_FLAG) == 0)
        return i;
    }

    return -1;
  }

  public int findLastRecord(int position) {
    int indexesLength = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer
        + PAGE_INDEXES_LENGTH_OFFSET);

    int endIndex = Math.min(indexesLength - 1, position);
    for (int i = endIndex; i >= 0; i--) {
      int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * i;
      int entryPointer = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + entryIndexPosition);
      if ((entryPointer & MARKED_AS_DELETED_FLAG) == 0)
        return i;
    }

    return -1;
  }

  public int getFreeSpace() {
    return OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + FREE_SPACE_COUNTER_OFFSET);
  }

  public int getMaxRecordSize() {
    int freeListHeader = OIntegerSerializer.INSTANCE
        .deserializeFromDirectMemory(directMemory, pagePointer + FREELIST_HEADER_OFFSET);

    int maxEntrySize;
    if (freeListHeader > 0)
      maxEntrySize = getFreeSpace();
    else
      maxEntrySize = getFreeSpace() - INDEX_ITEM_SIZE;

    return maxEntrySize - 2 * OIntegerSerializer.INT_SIZE;
  }

  public int getRecordsCount() {
    return OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + ENTRIES_COUNT_OFFSET);
  }

  public long getNextPage() {
    return OLongSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + NEXT_PAGE_OFFSET);
  }

  public void setNextPage(long nextPage) {
    OLongSerializer.INSTANCE.serializeInDirectMemory(nextPage, directMemory, pagePointer + NEXT_PAGE_OFFSET);
  }

  public long getPrevPage() {
    return OLongSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + PREV_PAGE_OFFSET);
  }

  public void setPrevPage(long prevPage) {
    OLongSerializer.INSTANCE.serializeInDirectMemory(prevPage, directMemory, pagePointer + PREV_PAGE_OFFSET);
  }

  private void incrementEntriesCount() {
    OIntegerSerializer.INSTANCE.serializeInDirectMemory(getRecordsCount() + 1, directMemory, pagePointer + ENTRIES_COUNT_OFFSET);
  }

  private void decrementEntriesCount() {
    OIntegerSerializer.INSTANCE.serializeInDirectMemory(getRecordsCount() - 1, directMemory, pagePointer + ENTRIES_COUNT_OFFSET);
  }

  private void doDefragmentation() {
    int freePosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + FREE_POSITION_OFFSET);
    int currentPosition = freePosition;
    List<Integer> processedPositions = new ArrayList<Integer>();

    while (currentPosition < PAGE_SIZE) {
      int entrySize = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + currentPosition);

      if (entrySize > 0) {
        int positionIndex = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + currentPosition
            + OIntegerSerializer.INT_SIZE);
        processedPositions.add(positionIndex);

        currentPosition += entrySize;
      } else {
        entrySize = -entrySize;
        directMemory.copyData(pagePointer + freePosition, pagePointer + freePosition + entrySize, currentPosition - freePosition);
        currentPosition += entrySize;
        freePosition += entrySize;

        shiftPositions(processedPositions, entrySize);
      }
    }

    OIntegerSerializer.INSTANCE.serializeInDirectMemory(freePosition, directMemory, pagePointer + FREE_POSITION_OFFSET);
  }

  private void shiftPositions(List<Integer> processedPositions, int entrySize) {
    for (int positionIndex : processedPositions) {
      int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * positionIndex;
      int entryPosition = OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(directMemory, pagePointer + entryIndexPosition);
      OIntegerSerializer.INSTANCE
          .serializeInDirectMemory(entryPosition + entrySize, directMemory, pagePointer + entryIndexPosition);
    }
  }
}

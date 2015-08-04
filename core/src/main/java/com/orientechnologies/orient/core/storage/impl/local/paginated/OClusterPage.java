/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;
import com.orientechnologies.orient.core.version.ORecordVersion;
import com.orientechnologies.orient.core.version.OVersionFactory;

/**
 * @author Andrey Lomakin
 * @since 19.03.13
 */
public class OClusterPage extends ODurablePage {

  private static final int VERSION_SIZE = OVersionFactory.instance().getVersionSize();

  private static final int NEXT_PAGE_OFFSET = NEXT_FREE_POSITION;
  private static final int PREV_PAGE_OFFSET = NEXT_PAGE_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int FREELIST_HEADER_OFFSET     = PREV_PAGE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int FREE_POSITION_OFFSET       = FREELIST_HEADER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int FREE_SPACE_COUNTER_OFFSET  = FREE_POSITION_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int ENTRIES_COUNT_OFFSET       = FREE_SPACE_COUNTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int PAGE_INDEXES_LENGTH_OFFSET = ENTRIES_COUNT_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int PAGE_INDEXES_OFFSET        = PAGE_INDEXES_LENGTH_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final int INDEX_ITEM_SIZE        = OIntegerSerializer.INT_SIZE + VERSION_SIZE;
  private static final int MARKED_AS_DELETED_FLAG = 1 << 16;
  private static final int POSITION_MASK          = 0xFFFF;
  public static final int  PAGE_SIZE              = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  public static final int MAX_ENTRY_SIZE = PAGE_SIZE - PAGE_INDEXES_OFFSET - INDEX_ITEM_SIZE;

  public static final int MAX_RECORD_SIZE = MAX_ENTRY_SIZE - 3 * OIntegerSerializer.INT_SIZE;

  public OClusterPage(OCacheEntry cacheEntry, boolean newPage, OWALChangesTree changesTree) throws IOException {
    super(cacheEntry, changesTree);

    if (newPage) {
      setLongValue(NEXT_PAGE_OFFSET, -1);
      setLongValue(PREV_PAGE_OFFSET, -1);

      setIntValue(FREE_POSITION_OFFSET, PAGE_SIZE);
      setIntValue(FREE_SPACE_COUNTER_OFFSET, PAGE_SIZE - PAGE_INDEXES_OFFSET);
    }
  }

  public int appendRecord(ORecordVersion recordVersion, byte[] record) throws IOException {
    int freePosition = getIntValue(FREE_POSITION_OFFSET);
    int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);

    int lastEntryIndexPosition = PAGE_INDEXES_OFFSET + indexesLength * INDEX_ITEM_SIZE;

    int entrySize = record.length + 3 * OIntegerSerializer.INT_SIZE;
    int freeListHeader = getIntValue(FREELIST_HEADER_OFFSET);

    if (!checkSpace(entrySize, freeListHeader))
      return -1;

    if (freeListHeader > 0) {
      if (freePosition - entrySize < lastEntryIndexPosition)
        doDefragmentation();
    } else {
      if (freePosition - entrySize < lastEntryIndexPosition + INDEX_ITEM_SIZE)
        doDefragmentation();
    }

    freePosition = getIntValue(FREE_POSITION_OFFSET);
    freePosition -= entrySize;
    int entryIndex;

    if (freeListHeader > 0) {
      entryIndex = freeListHeader - 1;

      final int tombstonePointer = getIntValue(PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * entryIndex);

      int nextEntryPosition = tombstonePointer & POSITION_MASK;
      if (nextEntryPosition > 0)
        setIntValue(FREELIST_HEADER_OFFSET, nextEntryPosition);
      else
        setIntValue(FREELIST_HEADER_OFFSET, 0);

      setIntValue(FREE_SPACE_COUNTER_OFFSET, getFreeSpace() - entrySize);

      int entryIndexPosition = PAGE_INDEXES_OFFSET + entryIndex * INDEX_ITEM_SIZE;
      setIntValue(entryIndexPosition, freePosition);

      byte[] serializedVersion = new byte[OVersionFactory.instance().getVersionSize()];
      recordVersion.getSerializer().fastWriteTo(serializedVersion, 0, recordVersion);
      setBinaryValue(entryIndexPosition + OIntegerSerializer.INT_SIZE, serializedVersion);
    } else {
      entryIndex = indexesLength;

      setIntValue(PAGE_INDEXES_LENGTH_OFFSET, indexesLength + 1);
      setIntValue(FREE_SPACE_COUNTER_OFFSET, getFreeSpace() - entrySize - INDEX_ITEM_SIZE);

      int entryIndexPosition = PAGE_INDEXES_OFFSET + entryIndex * INDEX_ITEM_SIZE;
      setIntValue(entryIndexPosition, freePosition);

      byte[] serializedVersion = new byte[OVersionFactory.instance().getVersionSize()];
      recordVersion.getSerializer().fastWriteTo(serializedVersion, 0, recordVersion);
      setBinaryValue(entryIndexPosition + OIntegerSerializer.INT_SIZE, serializedVersion);
    }

    int entryPosition = freePosition;
    setIntValue(entryPosition, entrySize);
    entryPosition += OIntegerSerializer.INT_SIZE;

    setIntValue(entryPosition, entryIndex);
    entryPosition += OIntegerSerializer.INT_SIZE;

    setIntValue(entryPosition, record.length);
    entryPosition += OIntegerSerializer.INT_SIZE;

    setBinaryValue(entryPosition, record);

    setIntValue(FREE_POSITION_OFFSET, freePosition);

    incrementEntriesCount();

    return entryIndex;
  }

  public int replaceRecord(int entryIndex, byte[] record, ORecordVersion recordVersion) throws IOException {
    int entryIndexPosition = PAGE_INDEXES_OFFSET + entryIndex * INDEX_ITEM_SIZE;

    if (recordVersion != null) {
      byte[] serializedVersion = getBinaryValue(entryIndexPosition + OIntegerSerializer.INT_SIZE,
          OVersionFactory.instance().getVersionSize());

      ORecordVersion storedRecordVersion = OVersionFactory.instance().createVersion();
      storedRecordVersion.getSerializer().fastReadFrom(serializedVersion, 0, storedRecordVersion);
      if (recordVersion.compareTo(storedRecordVersion) > 0) {
        recordVersion.getSerializer().fastWriteTo(serializedVersion, 0, recordVersion);
        setBinaryValue(entryIndexPosition + OIntegerSerializer.INT_SIZE, serializedVersion);
      }
    }

    int entryPointer = getIntValue(entryIndexPosition);
    int entryPosition = entryPointer & POSITION_MASK;

    int recordSize = getIntValue(entryPosition) - 3 * OIntegerSerializer.INT_SIZE;
    int writtenBytes;
    if (record.length <= recordSize) {
      setIntValue(entryPointer + 2 * OIntegerSerializer.INT_SIZE, record.length);
      setBinaryValue(entryPointer + 3 * OIntegerSerializer.INT_SIZE, record);
      writtenBytes = record.length;
    } else {
      byte[] newRecord = new byte[recordSize];
      System.arraycopy(record, 0, newRecord, 0, newRecord.length);
      setBinaryValue(entryPointer + 3 * OIntegerSerializer.INT_SIZE, newRecord);
      writtenBytes = newRecord.length;
    }

    return writtenBytes;
  }

  public ORecordVersion getRecordVersion(int position) {
    int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    if (position >= indexesLength)
      return null;

    int entryIndexPosition = PAGE_INDEXES_OFFSET + position * INDEX_ITEM_SIZE;
    byte[] serializedVersion = getBinaryValue(entryIndexPosition + OIntegerSerializer.INT_SIZE,
        OVersionFactory.instance().getVersionSize());

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

  public boolean deleteRecord(int position) throws IOException {
    int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    if (position >= indexesLength)
      return false;

    int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * position;
    int entryPointer = getIntValue(entryIndexPosition);

    if ((entryPointer & MARKED_AS_DELETED_FLAG) > 0)
      return false;

    int entryPosition = entryPointer & POSITION_MASK;

    int freeListHeader = getIntValue(FREELIST_HEADER_OFFSET);
    if (freeListHeader <= 0)
      setIntValue(entryIndexPosition, MARKED_AS_DELETED_FLAG);
    else
      setIntValue(entryIndexPosition, freeListHeader | MARKED_AS_DELETED_FLAG);

    setIntValue(FREELIST_HEADER_OFFSET, position + 1);

    final int entrySize = getIntValue(entryPosition);
    assert entrySize > 0;

    setIntValue(entryPosition, -entrySize);
    setIntValue(FREE_SPACE_COUNTER_OFFSET, getFreeSpace() + entrySize);

    decrementEntriesCount();

    return true;
  }

  public boolean isDeleted(int position) {
    int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    if (position >= indexesLength)
      return true;

    int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * position;
    int entryPointer = getIntValue(entryIndexPosition);

    return (entryPointer & MARKED_AS_DELETED_FLAG) > 0;
  }

  public int getRecordSize(int position) {
    int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    if (position >= indexesLength)
      return -1;

    int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * position;
    int entryPointer = getIntValue(entryIndexPosition);
    if ((entryPointer & MARKED_AS_DELETED_FLAG) > 0)
      return -1;

    int entryPosition = entryPointer & POSITION_MASK;
    return getIntValue(entryPosition + 2 * OIntegerSerializer.INT_SIZE);
  }

  public int findFirstDeletedRecord(int position) {
    int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    for (int i = position; i < indexesLength; i++) {
      int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * i;
      int entryPointer = getIntValue(entryIndexPosition);
      if ((entryPointer & MARKED_AS_DELETED_FLAG) > 0)
        return i;
    }

    return -1;
  }

  public int findFirstRecord(int position) {
    int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    for (int i = position; i < indexesLength; i++) {
      int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * i;
      int entryPointer = getIntValue(entryIndexPosition);
      if ((entryPointer & MARKED_AS_DELETED_FLAG) == 0)
        return i;
    }

    return -1;
  }

  public int findLastRecord(int position) {
    int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);

    int endIndex = Math.min(indexesLength - 1, position);
    for (int i = endIndex; i >= 0; i--) {
      int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * i;
      int entryPointer = getIntValue(entryIndexPosition);
      if ((entryPointer & MARKED_AS_DELETED_FLAG) == 0)
        return i;
    }

    return -1;
  }

  public int getFreeSpace() {
    return getIntValue(FREE_SPACE_COUNTER_OFFSET);
  }

  public int getMaxRecordSize() {
    int freeListHeader = getIntValue(FREELIST_HEADER_OFFSET);

    int maxEntrySize;
    if (freeListHeader > 0)
      maxEntrySize = getFreeSpace();
    else
      maxEntrySize = getFreeSpace() - INDEX_ITEM_SIZE;

    int result = maxEntrySize - 3 * OIntegerSerializer.INT_SIZE;
    if (result < 0)
      return 0;

    return result;
  }

  public int getRecordsCount() {
    return getIntValue(ENTRIES_COUNT_OFFSET);
  }

  public long getNextPage() {
    return getLongValue(NEXT_PAGE_OFFSET);
  }

  public void setNextPage(long nextPage) throws IOException {
    setLongValue(NEXT_PAGE_OFFSET, nextPage);
  }

  public long getPrevPage() {
    return getLongValue(PREV_PAGE_OFFSET);
  }

  public void setPrevPage(long prevPage) throws IOException {
    setLongValue(PREV_PAGE_OFFSET, prevPage);
  }

  public void setRecordLongValue(int recordPosition, int offset, long value) throws IOException {
    assert isPositionInsideInterval(recordPosition);

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + recordPosition * INDEX_ITEM_SIZE;
    final int entryPointer = getIntValue(entryIndexPosition);
    final int entryPosition = entryPointer & POSITION_MASK;

    if (offset >= 0) {
      assert insideRecordBounds(entryPosition, offset, OLongSerializer.LONG_SIZE);
      setLongValue(entryPosition + offset + 3 * OIntegerSerializer.INT_SIZE, value);
    } else {
      final int recordSize = getIntValue(entryPosition + 2 * OIntegerSerializer.INT_SIZE);
      assert insideRecordBounds(entryPosition, recordSize + offset, OLongSerializer.LONG_SIZE);
      setLongValue(entryPosition + 3 * OIntegerSerializer.INT_SIZE + recordSize + offset, value);
    }
  }

  public long getRecordLongValue(int recordPosition, int offset) {
    assert isPositionInsideInterval(recordPosition);

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + recordPosition * INDEX_ITEM_SIZE;
    final int entryPointer = getIntValue(entryIndexPosition);
    final int entryPosition = entryPointer & POSITION_MASK;

    if (offset >= 0) {
      assert insideRecordBounds(entryPosition, offset, OLongSerializer.LONG_SIZE);
      return getLongValue(entryPosition + offset + 3 * OIntegerSerializer.INT_SIZE);
    } else {
      final int recordSize = getIntValue(entryPosition + 2 * OIntegerSerializer.INT_SIZE);
      assert insideRecordBounds(entryPosition, recordSize + offset, OLongSerializer.LONG_SIZE);
      return getLongValue(entryPosition + 3 * OIntegerSerializer.INT_SIZE + recordSize + offset);
    }
  }

  public byte[] getRecordBinaryValue(int recordPosition, int offset, int size) throws IOException {
    assert isPositionInsideInterval(recordPosition);

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + recordPosition * INDEX_ITEM_SIZE;
    final int entryPointer = getIntValue(entryIndexPosition);
    final int entryPosition = entryPointer & POSITION_MASK;

    if (offset >= 0) {
      assert insideRecordBounds(entryPosition, offset, size);

      return getBinaryValue(entryPosition + offset + 3 * OIntegerSerializer.INT_SIZE, size);
    } else {
      final int recordSize = getIntValue(entryPosition + 2 * OIntegerSerializer.INT_SIZE);
      assert insideRecordBounds(entryPosition, recordSize + offset, OLongSerializer.LONG_SIZE);

      return getBinaryValue(entryPosition + 3 * OIntegerSerializer.INT_SIZE + recordSize + offset, size);
    }
  }

  public byte getRecordByteValue(int recordPosition, int offset) {
    assert isPositionInsideInterval(recordPosition);

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + recordPosition * INDEX_ITEM_SIZE;
    final int entryPointer = getIntValue(entryIndexPosition);
    final int entryPosition = entryPointer & POSITION_MASK;

    if (offset >= 0) {
      assert insideRecordBounds(entryPosition, offset, OByteSerializer.BYTE_SIZE);
      return getByteValue(entryPosition + offset + 3 * OIntegerSerializer.INT_SIZE);
    } else {
      final int recordSize = getIntValue(entryPosition + 2 * OIntegerSerializer.INT_SIZE);
      assert insideRecordBounds(entryPosition, recordSize + offset, OByteSerializer.BYTE_SIZE);
      return getByteValue(entryPosition + 3 * OIntegerSerializer.INT_SIZE + recordSize + offset);
    }
  }

  private boolean insideRecordBounds(int entryPosition, int offset, int contentSize) {
    final int recordSize = getIntValue(entryPosition + 2 * OIntegerSerializer.INT_SIZE);

    if (offset < 0)
      return false;

    if (offset + contentSize > recordSize)
      return false;

    return true;
  }

  private void incrementEntriesCount() throws IOException {
    setIntValue(ENTRIES_COUNT_OFFSET, getRecordsCount() + 1);
  }

  private void decrementEntriesCount() throws IOException {
    setIntValue(ENTRIES_COUNT_OFFSET, getRecordsCount() - 1);
  }

  private boolean isPositionInsideInterval(int recordPosition) {
    int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    return recordPosition < indexesLength;
  }

  private void doDefragmentation() throws IOException {
    int freePosition = getIntValue(FREE_POSITION_OFFSET);

    int currentPosition = freePosition;
    List<Integer> processedPositions = new ArrayList<Integer>();

    while (currentPosition < PAGE_SIZE) {
      int entrySize = getIntValue(currentPosition);

      if (entrySize > 0) {
        int positionIndex = getIntValue(currentPosition + OIntegerSerializer.INT_SIZE);
        processedPositions.add(positionIndex);

        currentPosition += entrySize;
      } else {
        entrySize = -entrySize;
        moveData(freePosition, freePosition + entrySize, currentPosition - freePosition);
        currentPosition += entrySize;
        freePosition += entrySize;

        shiftPositions(processedPositions, entrySize);
      }
    }

    setIntValue(FREE_POSITION_OFFSET, freePosition);
  }

  private void shiftPositions(List<Integer> processedPositions, int entrySize) throws IOException {
    for (int positionIndex : processedPositions) {
      int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * positionIndex;
      int entryPosition = getIntValue(entryIndexPosition);
      setIntValue(entryIndexPosition, entryPosition + entrySize);
    }
  }

}

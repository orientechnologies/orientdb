/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.version;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import java.util.Set;

public final class OVersionPage extends ODurablePage {
  private static final int VERSION_SIZE = ORecordVersionHelper.SERIALIZED_SIZE;

  private static final int NEXT_PAGE_OFFSET = NEXT_FREE_POSITION;
  private static final int PREV_PAGE_OFFSET = NEXT_PAGE_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int FREELIST_HEADER_OFFSET = PREV_PAGE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int FREE_POSITION_OFFSET =
      FREELIST_HEADER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int FREE_SPACE_COUNTER_OFFSET =
      FREE_POSITION_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int ENTRIES_COUNT_OFFSET =
      FREE_SPACE_COUNTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int PAGE_INDEXES_LENGTH_OFFSET =
      ENTRIES_COUNT_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int PAGE_INDEXES_OFFSET =
      PAGE_INDEXES_LENGTH_OFFSET + OIntegerSerializer.INT_SIZE;

  static final int INDEX_ITEM_SIZE = OIntegerSerializer.INT_SIZE + VERSION_SIZE;
  private static final int MARKED_AS_DELETED_FLAG = 1 << 16;
  private static final int POSITION_MASK = 0xFFFF;
  public static final int PAGE_SIZE =
      OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  static final int MAX_ENTRY_SIZE = PAGE_SIZE - PAGE_INDEXES_OFFSET - INDEX_ITEM_SIZE;

  public static final int MAX_RECORD_SIZE = MAX_ENTRY_SIZE - 3 * OIntegerSerializer.INT_SIZE;

  private static final int ENTRY_KIND_HOLE = -1;
  private static final int ENTRY_KIND_UNKNOWN = 0;
  private static final int ENTRY_KIND_DATA = +1;

  public OVersionPage(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init() {
    setLongValue(NEXT_PAGE_OFFSET, -1);
    setLongValue(PREV_PAGE_OFFSET, -1);

    setIntValue(FREELIST_HEADER_OFFSET, 0);
    setIntValue(PAGE_INDEXES_LENGTH_OFFSET, 0);
    setIntValue(ENTRIES_COUNT_OFFSET, 0);

    setIntValue(FREE_POSITION_OFFSET, PAGE_SIZE);
    setIntValue(FREE_SPACE_COUNTER_OFFSET, PAGE_SIZE - PAGE_INDEXES_OFFSET);
  }

  public int appendRecord(
      final int recordVersion,
      final byte[] record,
      final int requestedPosition,
      final Set<Integer> bookedRecordPositions) {
    int freePosition = getIntValue(FREE_POSITION_OFFSET);
    final int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);

    final int lastEntryIndexPosition = PAGE_INDEXES_OFFSET + indexesLength * INDEX_ITEM_SIZE;

    int entrySize = record.length + 3 * OIntegerSerializer.INT_SIZE;
    int freeListHeader = getIntValue(FREELIST_HEADER_OFFSET);

    if (!checkSpace(entrySize)) {
      return -1;
    }

    if (freePosition - entrySize < lastEntryIndexPosition + INDEX_ITEM_SIZE) {
      doDefragmentation();
    }

    freePosition = getIntValue(FREE_POSITION_OFFSET);
    freePosition -= entrySize;

    int entryIndex;
    boolean allocatedFromFreeList;

    if (requestedPosition < 0) {
      final ORawPair<Integer, Boolean> entry =
          findFirstEmptySlot(
              recordVersion,
              freePosition,
              indexesLength,
              entrySize,
              freeListHeader,
              bookedRecordPositions);
      entryIndex = entry.first;
      allocatedFromFreeList = entry.second;
    } else {
      allocatedFromFreeList =
          insertIntoRequestedSlot(
              recordVersion, freePosition, entrySize, requestedPosition, freeListHeader);
      entryIndex = requestedPosition;
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

  private boolean insertIntoRequestedSlot(
      final int recordVersion,
      final int freePosition,
      final int entrySize,
      final int requestedPosition,
      final int freeListHeader) {
    boolean allocatedFromFreeList = false;
    int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    // 1. requested position is first free slot inside of list of pointers
    if (indexesLength == requestedPosition) {
      setIntValue(PAGE_INDEXES_LENGTH_OFFSET, indexesLength + 1);
      setIntValue(FREE_SPACE_COUNTER_OFFSET, getFreeSpace() - entrySize - INDEX_ITEM_SIZE);

      int entryIndexPosition = PAGE_INDEXES_OFFSET + requestedPosition * INDEX_ITEM_SIZE;
      setIntValue(entryIndexPosition, freePosition);

      setIntValue(entryIndexPosition + OIntegerSerializer.INT_SIZE, recordVersion);
    } else if (indexesLength > requestedPosition) {
      // 2 requested position inside of list of pointers
      final int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * requestedPosition;
      int entryPointer = getIntValue(entryIndexPosition);
      // 2.1 requested position already occupied by other record, should not really happen
      if ((entryPointer & MARKED_AS_DELETED_FLAG) == 0) {
        throw new OStorageException(
            "Can not insert record inside of already occupied slot, record position = "
                + requestedPosition);
      }

      // 2.2 requested position is already removed, read free list of removed pointers till we will
      // not find one which we need
      // remove
      int prevFreeListItem = -1;
      int currentFreeListItem = freeListHeader - 1;
      while (true) {
        final int tombstonePointer =
            getIntValue(PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * currentFreeListItem);
        final int nextEntryPosition = (tombstonePointer & POSITION_MASK);

        if (currentFreeListItem == requestedPosition) {
          if (prevFreeListItem >= 0) {
            setIntValue(PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * prevFreeListItem, tombstonePointer);
          } else {
            setIntValue(FREELIST_HEADER_OFFSET, nextEntryPosition);
          }

          break;
        }

        if (nextEntryPosition > 0) {
          prevFreeListItem = currentFreeListItem;
          currentFreeListItem = nextEntryPosition - 1;
        } else {
          throw new OStorageException(
              "Record position "
                  + requestedPosition
                  + " marked as deleted but can not be found in the list of deleted records");
        }
      }

      // insert record into acquired slot
      setIntValue(FREE_SPACE_COUNTER_OFFSET, getFreeSpace() - entrySize);
      setIntValue(entryIndexPosition, freePosition);

      setIntValue(entryIndexPosition + OIntegerSerializer.INT_SIZE, recordVersion);
      allocatedFromFreeList = true;
    } else {
      throw new OStorageException(
          "Can not insert record out side of list of already inserted records, record position = "
              + requestedPosition);
    }

    return allocatedFromFreeList;
  }

  private ORawPair<Integer, Boolean> findFirstEmptySlot(
      int recordVersion,
      int freePosition,
      int indexesLength,
      int entrySize,
      int freeListHeader,
      Set<Integer> bookedRecordPositions) {
    boolean allocatedFromFreeList = false;
    int entryIndex;
    if (freeListHeader > 0) {
      // iterate over free list of times to find first not booked position to reuse
      entryIndex = -1;

      int prevFreeListItem = -1;
      int currentFreeListItem = freeListHeader - 1;
      while (true) {
        final int tombstonePointer =
            getIntValue(PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * currentFreeListItem);
        final int nextEntryPosition = (tombstonePointer & POSITION_MASK);

        if (!bookedRecordPositions.contains(currentFreeListItem)) {
          if (prevFreeListItem >= 0) {
            setIntValue(PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * prevFreeListItem, tombstonePointer);
          } else {
            setIntValue(FREELIST_HEADER_OFFSET, nextEntryPosition);
          }

          entryIndex = currentFreeListItem;
          break;
        }

        if (nextEntryPosition > 0) {
          prevFreeListItem = currentFreeListItem;
          currentFreeListItem = nextEntryPosition - 1;
        } else {
          break;
        }
      }

      if (entryIndex >= 0) {
        setIntValue(FREE_SPACE_COUNTER_OFFSET, getFreeSpace() - entrySize);

        int entryIndexPosition = PAGE_INDEXES_OFFSET + entryIndex * INDEX_ITEM_SIZE;
        setIntValue(entryIndexPosition, freePosition);

        setIntValue(entryIndexPosition + OIntegerSerializer.INT_SIZE, recordVersion);

        allocatedFromFreeList = true;
      } else {
        entryIndex = appendEntry(recordVersion, freePosition, indexesLength, entrySize);
      }
    } else {
      entryIndex = appendEntry(recordVersion, freePosition, indexesLength, entrySize);
    }

    return new ORawPair<>(entryIndex, allocatedFromFreeList);
  }

  private int appendEntry(int recordVersion, int freePosition, int indexesLength, int entrySize) {
    int entryIndex;
    entryIndex = indexesLength;

    setIntValue(PAGE_INDEXES_LENGTH_OFFSET, indexesLength + 1);
    setIntValue(FREE_SPACE_COUNTER_OFFSET, getFreeSpace() - entrySize - INDEX_ITEM_SIZE);

    int entryIndexPosition = PAGE_INDEXES_OFFSET + entryIndex * INDEX_ITEM_SIZE;
    setIntValue(entryIndexPosition, freePosition);

    setIntValue(entryIndexPosition + OIntegerSerializer.INT_SIZE, recordVersion);
    return entryIndex;
  }

  public byte[] replaceRecord(int entryIndex, byte[] record, final int recordVersion) {
    int entryIndexPosition = PAGE_INDEXES_OFFSET + entryIndex * INDEX_ITEM_SIZE;

    if (recordVersion != -1) {
      setIntValue(entryIndexPosition + OIntegerSerializer.INT_SIZE, recordVersion);
    }

    int entryPointer = getIntValue(entryIndexPosition);
    int entryPosition = entryPointer & POSITION_MASK;

    int recordSize = getIntValue(entryPosition) - 3 * OIntegerSerializer.INT_SIZE;
    if (recordSize != record.length) {
      throw new IllegalStateException(
          "Length of passed in and stored records are different. Stored record length = "
              + recordSize
              + ", passed record length = "
              + record.length);
    }

    final byte[] oldRecord =
        getBinaryValue(entryPointer + 3 * OIntegerSerializer.INT_SIZE, recordSize);

    setIntValue(entryPointer + 2 * OIntegerSerializer.INT_SIZE, record.length);
    setBinaryValue(entryPointer + 3 * OIntegerSerializer.INT_SIZE, record);

    return oldRecord;
  }

  public int getRecordVersion(int position) {
    int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    if (position >= indexesLength) {
      return -1;
    }

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + position * INDEX_ITEM_SIZE;
    final int entryPointer = getIntValue(entryIndexPosition);

    if ((entryPointer & MARKED_AS_DELETED_FLAG) != 0) {
      return -1;
    }
    return getIntValue(entryIndexPosition + OIntegerSerializer.INT_SIZE);
  }

  public boolean isEmpty() {
    return getFreeSpace() == PAGE_SIZE - PAGE_INDEXES_OFFSET;
  }

  private boolean checkSpace(int entrySize) {
    return getFreeSpace() - entrySize >= 0;
  }

  public byte[] deleteRecord(int position, boolean preserveFreeListPointer) {
    int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    if (position >= indexesLength) {
      return null;
    }

    int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * position;
    final int entryPointer = getIntValue(entryIndexPosition);

    if ((entryPointer & MARKED_AS_DELETED_FLAG) != 0) {
      return null;
    }

    int entryPosition = entryPointer & POSITION_MASK;

    if (preserveFreeListPointer) {
      int freeListHeader = getIntValue(FREELIST_HEADER_OFFSET);
      if (freeListHeader <= 0) {
        setIntValue(entryIndexPosition, MARKED_AS_DELETED_FLAG);
      } else {
        setIntValue(entryIndexPosition, freeListHeader | MARKED_AS_DELETED_FLAG);
      }

      setIntValue(FREELIST_HEADER_OFFSET, position + 1);
    } else {
      if (position != indexesLength - 1) {
        throw new IllegalStateException(
            "Only last position can be removed without keeping it in free list");
      }

      setIntValue(PAGE_INDEXES_LENGTH_OFFSET, indexesLength - 1);
      setIntValue(FREE_SPACE_COUNTER_OFFSET, getFreeSpace() + INDEX_ITEM_SIZE);
    }

    final int entrySize = getIntValue(entryPosition);
    assert entrySize > 0;

    setIntValue(entryPosition, -entrySize);
    setIntValue(FREE_SPACE_COUNTER_OFFSET, getFreeSpace() + entrySize);

    decrementEntriesCount();

    final byte[] oldRecord =
        getBinaryValue(
            entryPosition + 3 * OIntegerSerializer.INT_SIZE,
            entrySize - 3 * OIntegerSerializer.INT_SIZE);

    return oldRecord;
  }

  public boolean isDeleted(final int position) {
    final int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    if (position >= indexesLength) {
      return true;
    }

    int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * position;
    int entryPointer = getIntValue(entryIndexPosition);

    return (entryPointer & MARKED_AS_DELETED_FLAG) != 0;
  }

  public int getRecordSize(final int position) {
    final int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    if (position >= indexesLength) {
      return -1;
    }

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * position;
    final int entryPointer = getIntValue(entryIndexPosition);
    if ((entryPointer & MARKED_AS_DELETED_FLAG) != 0) {
      return -1;
    }

    final int entryPosition = entryPointer & POSITION_MASK;
    return getIntValue(entryPosition + 2 * OIntegerSerializer.INT_SIZE);
  }

  int findFirstDeletedRecord(final int position) {
    final int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    for (int i = position; i < indexesLength; i++) {
      int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * i;
      int entryPointer = getIntValue(entryIndexPosition);
      if ((entryPointer & MARKED_AS_DELETED_FLAG) != 0) {
        return i;
      }
    }

    return -1;
  }

  int findFirstRecord(final int position) {
    final int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    for (int i = position; i < indexesLength; i++) {
      final int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * i;
      final int entryPointer = getIntValue(entryIndexPosition);
      if ((entryPointer & MARKED_AS_DELETED_FLAG) == 0) {
        return i;
      }
    }

    return -1;
  }

  int findLastRecord(final int position) {
    final int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);

    final int endIndex = Math.min(indexesLength - 1, position);
    for (int i = endIndex; i >= 0; i--) {
      final int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * i;
      final int entryPointer = getIntValue(entryIndexPosition);
      if ((entryPointer & MARKED_AS_DELETED_FLAG) == 0) {
        return i;
      }
    }

    return -1;
  }

  public final int getFreeSpace() {
    return getIntValue(FREE_SPACE_COUNTER_OFFSET);
  }

  public int getMaxRecordSize() {
    final int maxEntrySize = getFreeSpace() - INDEX_ITEM_SIZE;

    final int result = maxEntrySize - 3 * OIntegerSerializer.INT_SIZE;
    return Math.max(result, 0);
  }

  public final int getRecordsCount() {
    return getIntValue(ENTRIES_COUNT_OFFSET);
  }

  public long getNextPage() {
    return getLongValue(NEXT_PAGE_OFFSET);
  }

  public void setNextPage(final long nextPage) {
    setLongValue(NEXT_PAGE_OFFSET, nextPage);
  }

  public long getPrevPage() {
    return getLongValue(PREV_PAGE_OFFSET);
  }

  public void setPrevPage(final long prevPage) {
    setLongValue(PREV_PAGE_OFFSET, prevPage);
  }

  public void setRecordLongValue(final int recordPosition, final int offset, final long value) {
    assert isPositionInsideInterval(recordPosition);

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + recordPosition * INDEX_ITEM_SIZE;
    final int entryPointer = getIntValue(entryIndexPosition);
    final int entryPosition = entryPointer & POSITION_MASK;

    if (offset >= 0) {
      assert insideRecordBounds(entryPosition, offset, OLongSerializer.LONG_SIZE);
      final int valueOffset = entryPosition + offset + 3 * OIntegerSerializer.INT_SIZE;
      setLongValue(valueOffset, value);
    } else {
      final int recordSize = getIntValue(entryPosition + 2 * OIntegerSerializer.INT_SIZE);
      assert insideRecordBounds(entryPosition, recordSize + offset, OLongSerializer.LONG_SIZE);
      final int valueOffset = entryPosition + 3 * OIntegerSerializer.INT_SIZE + recordSize + offset;
      setLongValue(valueOffset, value);
    }
  }

  public long getRecordLongValue(final int recordPosition, final int offset) {
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

  public byte[] getRecordBinaryValue(final int recordPosition, final int offset, final int size) {
    assert isPositionInsideInterval(recordPosition);

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + recordPosition * INDEX_ITEM_SIZE;
    final int entryPointer = getIntValue(entryIndexPosition);
    if ((entryPointer & MARKED_AS_DELETED_FLAG) != 0) {
      return null;
    }

    final int entryPosition = entryPointer & POSITION_MASK;

    if (offset >= 0) {
      assert insideRecordBounds(entryPosition, offset, size);

      return getBinaryValue(entryPosition + offset + 3 * OIntegerSerializer.INT_SIZE, size);
    } else {
      final int recordSize = getIntValue(entryPosition + 2 * OIntegerSerializer.INT_SIZE);
      assert insideRecordBounds(entryPosition, recordSize + offset, OLongSerializer.LONG_SIZE);

      return getBinaryValue(
          entryPosition + 3 * OIntegerSerializer.INT_SIZE + recordSize + offset, size);
    }
  }

  public byte getRecordByteValue(final int recordPosition, final int offset) {
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

  private boolean insideRecordBounds(
      final int entryPosition, final int offset, final int contentSize) {
    final int recordSize = getIntValue(entryPosition + 2 * OIntegerSerializer.INT_SIZE);
    return offset >= 0 && offset + contentSize <= recordSize;
  }

  private void incrementEntriesCount() {
    setIntValue(ENTRIES_COUNT_OFFSET, getRecordsCount() + 1);
  }

  private void decrementEntriesCount() {
    setIntValue(ENTRIES_COUNT_OFFSET, getRecordsCount() - 1);
  }

  private boolean isPositionInsideInterval(final int recordPosition) {
    final int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    return recordPosition < indexesLength;
  }

  private void doDefragmentation() {
    final int recordsCount = getRecordsCount();
    final int freePosition = getIntValue(FREE_POSITION_OFFSET);

    // 1. Build the entries "map" and merge consecutive holes.

    final int maxEntries =
        recordsCount /* live records */ + recordsCount + 1 /* max holes after merging */;
    final int[] positions = new int[maxEntries];
    final int[] sizes = new int[maxEntries];

    int count = 0;
    int currentPosition = freePosition;
    int lastEntryKind = ENTRY_KIND_UNKNOWN;
    while (currentPosition < PAGE_SIZE) {
      final int size = getIntValue(currentPosition);
      final int entryKind = Integer.signum(size);
      assert entryKind != ENTRY_KIND_UNKNOWN;

      if (entryKind == ENTRY_KIND_HOLE && lastEntryKind == ENTRY_KIND_HOLE) {
        sizes[count - 1] += size;
      } else {
        positions[count] = currentPosition;
        sizes[count] = size;

        ++count;

        lastEntryKind = entryKind;
      }

      currentPosition += entryKind == ENTRY_KIND_HOLE ? -size : size;
    }

    // 2. Iterate entries in reverse, update data offsets, merge consecutive data segments and move
    // them in a single operation.

    int shift = 0;
    int lastDataPosition = 0;
    int mergedDataSize = 0;
    for (int i = count - 1; i >= 0; --i) {
      final int position = positions[i];
      final int size = sizes[i];

      final int entryKind = Integer.signum(size);
      assert entryKind != ENTRY_KIND_UNKNOWN;

      if (entryKind == ENTRY_KIND_DATA && shift > 0) {
        final int positionIndex = getIntValue(position + OIntegerSerializer.INT_SIZE);
        setIntValue(PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * positionIndex, position + shift);

        lastDataPosition = position;
        mergedDataSize += size; // accumulate consecutive data segments size
      }

      if (mergedDataSize > 0
          && (entryKind == ENTRY_KIND_HOLE
              || i == 0)) { // move consecutive merged data segments in one go
        moveData(lastDataPosition, lastDataPosition + shift, mergedDataSize);
        mergedDataSize = 0;
      }

      if (entryKind == ENTRY_KIND_HOLE) {
        shift += -size;
      }
    }

    // 3. Update free position.

    setIntValue(FREE_POSITION_OFFSET, freePosition + shift);
  }
}

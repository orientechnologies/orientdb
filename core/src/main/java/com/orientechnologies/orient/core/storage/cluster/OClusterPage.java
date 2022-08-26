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

package com.orientechnologies.orient.core.storage.cluster;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import java.util.Objects;
import java.util.Set;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 19.03.13
 */
public final class OClusterPage extends ODurablePage {

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

  public OClusterPage(OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init() {
    setLongValue(NEXT_PAGE_OFFSET, -1);
    setLongValue(PREV_PAGE_OFFSET, -1);

    setFreeListHeader(0);
    setPageIndexesLength(0);
    setIntValue(ENTRIES_COUNT_OFFSET, 0);

    setFreePosition(PAGE_SIZE);
    setFreeSpace(PAGE_SIZE - PAGE_INDEXES_OFFSET);
  }

  public int appendRecord(
      final int recordVersion,
      final byte[] record,
      final int requestedPosition,
      final Set<Integer> bookedRecordPositions) {
    int freePosition = getFreePosition();
    final int indexesLength = getPageIndexesLength();

    final int lastEntryIndexPosition = computePointerPosition(indexesLength);

    int entrySize = record.length + 3 * OIntegerSerializer.INT_SIZE;
    int freeListHeader = getFreeListHeader();

    if (!checkSpace(entrySize)) {
      return -1;
    }

    if (freePosition - entrySize < lastEntryIndexPosition + INDEX_ITEM_SIZE) {
      if (requestedPosition < 0) {
        final int[] findHole = findHole(entrySize, freePosition);

        if (findHole != null) {
          final int entryPosition = findHole[0];
          final int holeSize = findHole[1];

          final ORawPair<Integer, Boolean> entry =
              findFirstEmptySlot(
                  recordVersion,
                  entryPosition,
                  indexesLength,
                  entrySize,
                  freeListHeader,
                  true,
                  bookedRecordPositions);

          if (entry != null) {
            assert entry.second; // allocated from free list

            final int entryIndex = entry.first;

            assert holeSize >= entrySize;
            if (holeSize != entrySize) {
              setIntValue(entryPosition + entrySize, -(holeSize - entrySize));
            }

            writeEntry(record, entryPosition, entrySize, entryIndex);

            return entryIndex;
          }
        }
      }

      doDefragmentation();
    }

    freePosition = getFreePosition();
    freePosition -= entrySize;

    int entryIndex;

    if (requestedPosition < 0) {
      final ORawPair<Integer, Boolean> entry =
          findFirstEmptySlot(
              recordVersion,
              freePosition,
              indexesLength,
              entrySize,
              freeListHeader,
              false,
              bookedRecordPositions);
      Objects.requireNonNull(entry);
      entryIndex = entry.first;
    } else {
      insertIntoRequestedSlot(
          recordVersion, freePosition, entrySize, requestedPosition, freeListHeader);
      entryIndex = requestedPosition;
    }

    writeEntry(record, freePosition, entrySize, entryIndex);

    setFreePosition(freePosition);

    return entryIndex;
  }

  private void writeEntry(byte[] record, int entryPosition, int entrySize, int entryIndex) {
    assert entryPosition + entrySize <= PAGE_SIZE;

    setRecordEntrySize(entryPosition, entrySize);
    setRecordEntryIndex(entryPosition, entryIndex);
    setRecordEntryBytesLength(entryPosition, record.length);
    setRecordEntryBytes(entryPosition, record);

    incrementEntriesCount();
  }

  public void setRecordEntrySize(int recordPosition, int size) {
    setIntValue(recordPosition, size);
  }

  public void setRecordEntryIndex(int recordPosition, int index) {
    setIntValue(recordPosition + OIntegerSerializer.INT_SIZE, index);
  }

  public void setRecordEntryBytesLength(int recordPosition, int bytesLength) {
    setIntValue(recordPosition + OIntegerSerializer.INT_SIZE * 2, bytesLength);
  }

  public void setRecordEntryBytes(int recordPosition, byte[] record) {
    setBinaryValue(recordPosition + OIntegerSerializer.INT_SIZE * 3, record);
  }

  public int getRecordEntrySize(int recordPosition) {
    return getIntValue(recordPosition);
  }

  public int getRecordEntryIndex(int recordPosition) {
    return getIntValue(recordPosition + OIntegerSerializer.INT_SIZE);
  }

  public int getRecordEntryBytesLength(int recordPosition) {
    return getIntValue(recordPosition + OIntegerSerializer.INT_SIZE * 2);
  }

  public byte[] getRecordEntryBytes(int recordPosition, int valLen) {
    return getBinaryValue(recordPosition + OIntegerSerializer.INT_SIZE * 3, valLen);
  }

  /**
   * Finds the hole position and its size which is equal or bigger than requested size.
   *
   * @return position and size of the hole.
   */
  private int[] findHole(int requestedSize, int freePosition) {
    int currentPosition = freePosition;

    int holeSize = 0;
    int initialHolePosition = 0;

    while (currentPosition < PAGE_SIZE) {
      final int size = getIntValue(currentPosition);

      if (size < 0) {
        final int currentSize = -size;

        if (initialHolePosition == 0) {
          initialHolePosition = currentPosition;
          holeSize = currentSize;
        } else {
          holeSize += currentSize;
        }

        if (holeSize >= requestedSize) {
          if (holeSize > requestedSize) {
            if (holeSize - requestedSize > Integer.BYTES) {
              return new int[] {initialHolePosition, holeSize};
            } else {
              currentPosition += Math.abs(size);
              continue;
            }
          }

          return new int[] {initialHolePosition, holeSize};
        }
      } else {
        holeSize = 0;
        initialHolePosition = 0;
      }

      currentPosition += Math.abs(size);
    }

    return null;
  }

  private boolean insertIntoRequestedSlot(
      final int recordVersion,
      final int freePosition,
      final int entrySize,
      final int requestedPosition,
      final int freeListHeader) {
    boolean allocatedFromFreeList = false;
    int indexesLength = getPageIndexesLength();
    // 1. requested position is first free slot inside of list of pointers
    if (indexesLength == requestedPosition) {
      setPageIndexesLength(indexesLength + 1);
      setFreeSpace(getFreeSpace() - entrySize - INDEX_ITEM_SIZE);

      int entryIndexPosition = computePointerPosition(requestedPosition);
      setPointerAt(entryIndexPosition, freePosition);

      setVersionAt(entryIndexPosition, recordVersion);
    } else if (indexesLength > requestedPosition) {
      // 2 requested position inside of list of pointers
      final int entryIndexPosition = computePointerPosition(requestedPosition);
      int entryPointer = getPointerAt(entryIndexPosition);
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
        final int tombstonePointer = getPointerAt(computePointerPosition(currentFreeListItem));
        final int nextEntryPosition = (tombstonePointer & POSITION_MASK);

        if (currentFreeListItem == requestedPosition) {
          if (prevFreeListItem >= 0) {
            setPointer(prevFreeListItem, tombstonePointer);
          } else {
            setFreeListHeader(nextEntryPosition);
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
      setFreeSpace(getFreeSpace() - entrySize);
      setPointerAt(entryIndexPosition, freePosition);

      setVersionAt(entryIndexPosition, recordVersion);
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
      int entryPosition,
      int indexesLength,
      int entrySize,
      int freeListHeader,
      boolean useOnlyFreeList,
      Set<Integer> bookedRecordPositions) {
    boolean allocatedFromFreeList = false;
    int entryIndex;
    if (freeListHeader > 0) {
      // iterate over free list of times to find first not booked position to reuse
      entryIndex = -1;

      int prevFreeListItem = -1;
      int currentFreeListItem = freeListHeader - 1;
      while (true) {
        final int tombstonePointer = getPointer(currentFreeListItem);
        final int nextEntryPosition = (tombstonePointer & POSITION_MASK);

        if (!bookedRecordPositions.contains(currentFreeListItem)) {
          if (prevFreeListItem >= 0) {
            setPointer(prevFreeListItem, tombstonePointer);
          } else {
            setFreeListHeader(nextEntryPosition);
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
        setFreeSpace(getFreeSpace() - entrySize);

        int entryIndexPosition = computePointerPosition(entryIndex);
        setPointerAt(entryIndexPosition, entryPosition);

        setVersionAt(entryIndexPosition, recordVersion);

        allocatedFromFreeList = true;
      } else {
        if (useOnlyFreeList) {
          return null;
        }

        entryIndex = appendEntry(recordVersion, entryPosition, indexesLength, entrySize);
      }
    } else {
      if (useOnlyFreeList) {
        return null;
      }

      entryIndex = appendEntry(recordVersion, entryPosition, indexesLength, entrySize);
    }

    return new ORawPair<>(entryIndex, allocatedFromFreeList);
  }

  private int appendEntry(int recordVersion, int freePosition, int indexesLength, int entrySize) {
    int entryIndex;
    entryIndex = indexesLength;

    setPageIndexesLength(indexesLength + 1);
    setFreeSpace(getFreeSpace() - entrySize - INDEX_ITEM_SIZE);

    int entryIndexPosition = computePointerPosition(entryIndex);
    setPointerAt(entryIndexPosition, freePosition);

    setVersionAt(entryIndexPosition, recordVersion);
    return entryIndex;
  }

  public byte[] replaceRecord(int entryIndex, byte[] record, final int recordVersion) {
    int entryIndexPosition = computePointerPosition(entryIndex);

    if (recordVersion != -1) {
      setVersionAt(entryIndexPosition, recordVersion);
    }

    int entryPosition = getPointerValuePositionAt(entryIndexPosition);

    int recordSize = getRecordEntrySize(entryPosition) - 3 * OIntegerSerializer.INT_SIZE;
    if (recordSize != record.length) {
      throw new IllegalStateException(
          "Length of passed in and stored records are different. Stored record length = "
              + recordSize
              + ", passed record length = "
              + record.length);
    }

    final byte[] oldRecord = getRecordEntryBytes(entryPosition, recordSize);

    setRecordEntryBytesLength(entryPosition, record.length);
    setRecordEntryBytes(entryPosition, record);

    return oldRecord;
  }

  public int getRecordVersion(int position) {
    int indexesLength = getPageIndexesLength();
    if (position >= indexesLength) {
      return -1;
    }

    final int entryIndexPosition = computePointerPosition(position);
    final int entryPointer = getPointerAt(entryIndexPosition);

    if ((entryPointer & MARKED_AS_DELETED_FLAG) != 0) {
      return -1;
    }
    return getVersionAt(entryIndexPosition);
  }

  public boolean isEmpty() {
    return getFreeSpace() == PAGE_SIZE - PAGE_INDEXES_OFFSET;
  }

  private boolean checkSpace(int entrySize) {
    return getFreeSpace() - entrySize >= 0;
  }

  public byte[] deleteRecord(int position, boolean preserveFreeListPointer) {
    int indexesLength = getPageIndexesLength();
    if (position >= indexesLength) {
      return null;
    }

    int entryIndexPosition = computePointerPosition(position);
    final int entryPointer = getPointerAt(entryIndexPosition);

    if ((entryPointer & MARKED_AS_DELETED_FLAG) != 0) {
      return null;
    }

    final int oldVersion = getVersionAt(entryIndexPosition);
    int entryPosition = entryPointer & POSITION_MASK;

    if (preserveFreeListPointer) {
      int freeListHeader = getFreeListHeader();
      if (freeListHeader <= 0) {
        setPointerAt(entryIndexPosition, MARKED_AS_DELETED_FLAG);
      } else {
        setPointerAt(entryIndexPosition, freeListHeader | MARKED_AS_DELETED_FLAG);
      }

      setFreeListHeader(position + 1);
    } else {
      if (position != indexesLength - 1) {
        throw new IllegalStateException(
            "Only last position can be removed without keeping it in free list");
      }

      setPageIndexesLength(indexesLength - 1);
      setFreeSpace(getFreeSpace() + INDEX_ITEM_SIZE);
    }

    final int entrySize = getRecordEntrySize(entryPosition);
    assert entrySize + entryPosition <= PAGE_SIZE;
    assert entrySize > 0;

    final int recordSize = getRecordEntryBytesLength(entryPosition);

    setRecordEntrySize(entryPosition, -entrySize);
    setFreeSpace(getFreeSpace() + entrySize);

    decrementEntriesCount();

    final byte[] oldRecord = getRecordEntryBytes(entryPosition, recordSize);

    return oldRecord;
  }

  public boolean isDeleted(final int position) {
    final int indexesLength = getPageIndexesLength();
    if (position >= indexesLength) {
      return true;
    }

    int entryPointer = getPointer(position);

    return (entryPointer & MARKED_AS_DELETED_FLAG) != 0;
  }

  public int getRecordSize(final int position) {
    final int indexesLength = getPageIndexesLength();
    if (position >= indexesLength) {
      return -1;
    }

    final int entryPointer = getPointer(position);
    if ((entryPointer & MARKED_AS_DELETED_FLAG) != 0) {
      return -1;
    }

    final int entryPosition = entryPointer & POSITION_MASK;
    assert getRecordEntrySize(entryPosition) + entryPosition <= PAGE_SIZE;
    assert getRecordEntryBytesLength(entryPosition) <= PAGE_SIZE;

    return getRecordEntryBytesLength(entryPosition);
  }

  int findFirstDeletedRecord(final int position) {
    final int indexesLength = getPageIndexesLength();
    for (int i = position; i < indexesLength; i++) {
      int entryPointer = getPointer(i);
      if ((entryPointer & MARKED_AS_DELETED_FLAG) != 0) {
        return i;
      }
    }

    return -1;
  }

  int findFirstRecord(final int position) {
    final int indexesLength = getPageIndexesLength();
    for (int i = position; i < indexesLength; i++) {
      final int entryPointer = getPointer(i);
      if ((entryPointer & MARKED_AS_DELETED_FLAG) == 0) {
        return i;
      }
    }

    return -1;
  }

  int findLastRecord(final int position) {
    final int indexesLength = getPageIndexesLength();

    final int endIndex = Math.min(indexesLength - 1, position);
    for (int i = endIndex; i >= 0; i--) {
      final int entryPointer = getPointer(i);
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

    final int entryPosition = getPointerValuePosition(recordPosition);

    if (offset >= 0) {
      assert insideRecordBounds(entryPosition, offset, OLongSerializer.LONG_SIZE);
      final int valueOffset = entryPosition + offset + 3 * OIntegerSerializer.INT_SIZE;
      setLongValue(valueOffset, value);
    } else {
      final int recordSize = getRecordEntryBytesLength(entryPosition);
      assert insideRecordBounds(entryPosition, recordSize + offset, OLongSerializer.LONG_SIZE);
      final int valueOffset = entryPosition + 3 * OIntegerSerializer.INT_SIZE + recordSize + offset;
      setLongValue(valueOffset, value);
    }
  }

  public long getRecordLongValue(final int recordPosition, final int offset) {
    assert isPositionInsideInterval(recordPosition);

    final int entryPosition = getPointerValuePosition(recordPosition);

    if (offset >= 0) {
      assert insideRecordBounds(entryPosition, offset, OLongSerializer.LONG_SIZE);
      return getLongValue(entryPosition + offset + 3 * OIntegerSerializer.INT_SIZE);
    } else {
      final int recordSize = getRecordEntryBytesLength(entryPosition);
      assert insideRecordBounds(entryPosition, recordSize + offset, OLongSerializer.LONG_SIZE);
      return getLongValue(entryPosition + 3 * OIntegerSerializer.INT_SIZE + recordSize + offset);
    }
  }

  public byte[] getRecordBinaryValue(final int recordPosition, final int offset, final int size) {
    assert isPositionInsideInterval(recordPosition);

    final int entryPointer = getPointer(recordPosition);
    if ((entryPointer & MARKED_AS_DELETED_FLAG) != 0) {
      return null;
    }

    final int entryPosition = entryPointer & POSITION_MASK;

    if (offset >= 0) {
      assert insideRecordBounds(entryPosition, offset, size);

      return getBinaryValue(entryPosition + offset + 3 * OIntegerSerializer.INT_SIZE, size);
    } else {
      final int recordSize = getRecordEntryBytesLength(entryPosition);
      assert insideRecordBounds(entryPosition, recordSize + offset, OLongSerializer.LONG_SIZE);

      return getBinaryValue(
          entryPosition + 3 * OIntegerSerializer.INT_SIZE + recordSize + offset, size);
    }
  }

  public byte getRecordByteValue(final int recordPosition, final int offset) {
    assert isPositionInsideInterval(recordPosition);

    final int entryPosition = getPointerValuePosition(recordPosition);

    if (offset >= 0) {
      assert insideRecordBounds(entryPosition, offset, OByteSerializer.BYTE_SIZE);
      return getByteValue(entryPosition + offset + 3 * OIntegerSerializer.INT_SIZE);
    } else {
      final int recordSize = getRecordEntryBytesLength(entryPosition);
      assert insideRecordBounds(entryPosition, recordSize + offset, OByteSerializer.BYTE_SIZE);
      return getByteValue(entryPosition + 3 * OIntegerSerializer.INT_SIZE + recordSize + offset);
    }
  }

  private boolean insideRecordBounds(
      final int entryPosition, final int offset, final int contentSize) {
    final int recordSize = getRecordEntryBytesLength(entryPosition);
    return offset >= 0 && offset + contentSize <= recordSize;
  }

  private void incrementEntriesCount() {
    setIntValue(ENTRIES_COUNT_OFFSET, getRecordsCount() + 1);
  }

  private void decrementEntriesCount() {
    setIntValue(ENTRIES_COUNT_OFFSET, getRecordsCount() - 1);
  }

  private boolean isPositionInsideInterval(final int recordPosition) {
    final int indexesLength = getPageIndexesLength();
    return recordPosition < indexesLength;
  }

  private void doDefragmentation() {
    final int recordsCount = getRecordsCount();
    final int freePosition = getFreePosition();

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
        setPointer(positionIndex, position + shift);

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

    setFreePosition(freePosition + shift);
  }

  public int getFreePosition() {
    return getIntValue(FREE_POSITION_OFFSET);
  }

  public int setFreePosition(int freePosition) {
    return setIntValue(FREE_POSITION_OFFSET, freePosition);
  }

  public int setFreeSpace(int freePosition) {
    return setIntValue(FREE_SPACE_COUNTER_OFFSET, freePosition);
  }

  public int getPageIndexesLength() {
    return getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
  }

  public int setPageIndexesLength(int freePosition) {
    return setIntValue(PAGE_INDEXES_LENGTH_OFFSET, freePosition);
  }

  public void setPointer(int position, int value) {
    setPointerAt(computePointerPosition(position), value);
  }

  public void setPointerAt(int entryIndexPosition, int pointer) {
    setIntValue(entryIndexPosition, pointer);
  }

  public int getPointerAt(int entryIndexPosition) {
    return getIntValue(entryIndexPosition);
  }

  public int getPointerValuePositionAt(int entryIndexPosition) {
    int valuePosition = getPointerAt(entryIndexPosition);
    return valuePosition & POSITION_MASK;
  }

  public int getPointer(int position) {
    return getPointerAt(computePointerPosition(position));
  }

  public int getPointerValuePosition(int position) {
    return getPointerValuePositionAt(computePointerPosition(position));
  }

  public void setVersionAt(int entryIndexPosition, int version) {
    setIntValue(entryIndexPosition + OIntegerSerializer.INT_SIZE, version);
  }

  public int getVersionAt(int entryIndexPosition) {
    return getIntValue(entryIndexPosition + OIntegerSerializer.INT_SIZE);
  }

  public int computePointerPosition(int position) {
    return PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * position;
  }

  public int getFreeListHeader() {
    return getIntValue(FREELIST_HEADER_OFFSET);
  }

  public int setFreeListHeader(int freePosition) {
    return setIntValue(FREELIST_HEADER_OFFSET, freePosition);
  }
}

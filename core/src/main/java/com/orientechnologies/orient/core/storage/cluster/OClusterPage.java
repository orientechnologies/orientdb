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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 19.03.13
 */
public final class OClusterPage extends ODurablePage {

  private static final int VERSION_SIZE = ORecordVersionHelper.SERIALIZED_SIZE;

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
  public static final  int PAGE_SIZE              = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  public static final int MAX_ENTRY_SIZE = PAGE_SIZE - PAGE_INDEXES_OFFSET - INDEX_ITEM_SIZE;

  public static final int MAX_RECORD_SIZE = MAX_ENTRY_SIZE - 3 * OIntegerSerializer.INT_SIZE;

  private static final int ENTRY_KIND_HOLE    = -1;
  private static final int ENTRY_KIND_UNKNOWN = 0;
  private static final int ENTRY_KIND_DATA    = +1;

  public OClusterPage(OCacheEntry cacheEntry, boolean newPage) {
    super(cacheEntry);

    if (newPage) {
      setLongValue(NEXT_PAGE_OFFSET, -1);
      setLongValue(PREV_PAGE_OFFSET, -1);

      setIntValue(FREELIST_HEADER_OFFSET, 0);
      setIntValue(PAGE_INDEXES_LENGTH_OFFSET, 0);
      setIntValue(ENTRIES_COUNT_OFFSET, 0);

      setIntValue(FREE_POSITION_OFFSET, PAGE_SIZE);
      setIntValue(FREE_SPACE_COUNTER_OFFSET, PAGE_SIZE - PAGE_INDEXES_OFFSET);
    }
  }

  public int appendRecord(final int recordVersion, final byte[] record) {
    int freePosition = getIntValue(FREE_POSITION_OFFSET);
    final int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);

    final int lastEntryIndexPosition = PAGE_INDEXES_OFFSET + indexesLength * INDEX_ITEM_SIZE;

    int entrySize = record.length + 3 * OIntegerSerializer.INT_SIZE;
    int freeListHeader = getIntValue(FREELIST_HEADER_OFFSET);

    if (!checkSpace(entrySize, freeListHeader)) {
      return -1;
    }

    if (freeListHeader > 0) {
      if (freePosition - entrySize < lastEntryIndexPosition) {
        doDefragmentation();
      }
    } else {
      if (freePosition - entrySize < lastEntryIndexPosition + INDEX_ITEM_SIZE) {
        doDefragmentation();
      }
    }

    freePosition = getIntValue(FREE_POSITION_OFFSET);
    freePosition -= entrySize;
    int entryIndex;

    if (freeListHeader > 0) {
      entryIndex = freeListHeader - 1;

      final int tombstonePointer = getIntValue(PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * entryIndex);

      int nextEntryPosition = tombstonePointer & POSITION_MASK;
      if (nextEntryPosition > 0) {
        setIntValue(FREELIST_HEADER_OFFSET, nextEntryPosition);
      } else {
        setIntValue(FREELIST_HEADER_OFFSET, 0);
      }

      setIntValue(FREE_SPACE_COUNTER_OFFSET, getFreeSpace() - entrySize);

      int entryIndexPosition = PAGE_INDEXES_OFFSET + entryIndex * INDEX_ITEM_SIZE;
      setIntValue(entryIndexPosition, freePosition);

      setIntValue(entryIndexPosition + OIntegerSerializer.INT_SIZE, recordVersion);
    } else {
      entryIndex = indexesLength;

      setIntValue(PAGE_INDEXES_LENGTH_OFFSET, indexesLength + 1);
      setIntValue(FREE_SPACE_COUNTER_OFFSET, getFreeSpace() - entrySize - INDEX_ITEM_SIZE);

      int entryIndexPosition = PAGE_INDEXES_OFFSET + entryIndex * INDEX_ITEM_SIZE;
      setIntValue(entryIndexPosition, freePosition);

      setIntValue(entryIndexPosition + OIntegerSerializer.INT_SIZE, recordVersion);
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

  public int replaceRecord(int entryIndex, byte[] record, final int recordVersion) {
    int entryIndexPosition = PAGE_INDEXES_OFFSET + entryIndex * INDEX_ITEM_SIZE;

    if (recordVersion != -1) {
      setIntValue(entryIndexPosition + OIntegerSerializer.INT_SIZE, recordVersion);
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

  public int getRecordVersion(int position) {
    int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    if (position >= indexesLength) {
      return -1;
    }

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + position * INDEX_ITEM_SIZE;
    return getIntValue(entryIndexPosition + OIntegerSerializer.INT_SIZE);
  }

  public boolean isEmpty() {
    return getFreeSpace() == PAGE_SIZE - PAGE_INDEXES_OFFSET;
  }

  private boolean checkSpace(int entrySize, int freeListHeader) {
    if (freeListHeader > 0) {
      if (getFreeSpace() - entrySize < 0) {
        return false;
      }
    } else {
      if (getFreeSpace() - entrySize - INDEX_ITEM_SIZE < 0) {
        return false;
      }
    }
    return true;
  }

  public boolean deleteRecord(int position) {
    int indexesLength = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    if (position >= indexesLength) {
      return false;
    }

    int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * position;
    int entryPointer = getIntValue(entryIndexPosition);

    if ((entryPointer & MARKED_AS_DELETED_FLAG) != 0) {
      return false;
    }

    int entryPosition = entryPointer & POSITION_MASK;

    int freeListHeader = getIntValue(FREELIST_HEADER_OFFSET);
    if (freeListHeader <= 0) {
      setIntValue(entryIndexPosition, MARKED_AS_DELETED_FLAG);
    } else {
      setIntValue(entryIndexPosition, freeListHeader | MARKED_AS_DELETED_FLAG);
    }

    setIntValue(FREELIST_HEADER_OFFSET, position + 1);

    final int entrySize = getIntValue(entryPosition);
    assert entrySize > 0;

    setIntValue(entryPosition, -entrySize);
    setIntValue(FREE_SPACE_COUNTER_OFFSET, getFreeSpace() + entrySize);

    decrementEntriesCount();

    return true;
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

  public int findFirstDeletedRecord(final int position) {
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

  public int findFirstRecord(final int position) {
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

  public int findLastRecord(final int position) {
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
    final int freeListHeader = getIntValue(FREELIST_HEADER_OFFSET);

    final int maxEntrySize;
    if (freeListHeader > 0) {
      maxEntrySize = getFreeSpace();
    } else {
      maxEntrySize = getFreeSpace() - INDEX_ITEM_SIZE;
    }

    final int result = maxEntrySize - 3 * OIntegerSerializer.INT_SIZE;
    if (result < 0) {
      return 0;
    }

    return result;
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
      setLongValue(entryPosition + offset + 3 * OIntegerSerializer.INT_SIZE, value);
    } else {
      final int recordSize = getIntValue(entryPosition + 2 * OIntegerSerializer.INT_SIZE);
      assert insideRecordBounds(entryPosition, recordSize + offset, OLongSerializer.LONG_SIZE);
      setLongValue(entryPosition + 3 * OIntegerSerializer.INT_SIZE + recordSize + offset, value);
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

  /**
   * Writes binary dump of this cluster page to the log.
   */
  public void dumpToLog() {
    final StringBuilder text = new StringBuilder();

    text.append("Dump of ").append(this).append('\n');
    text.append("Magic:\t\t\t").append(String.format("%016X", getLongValue(MAGIC_NUMBER_OFFSET))).append('\n');
    text.append("CRC32:\t\t\t").append(String.format("%08X", getIntValue(CRC32_OFFSET))).append('\n');
    text.append("WAL Segment:\t").append(String.format("%016X", getLongValue(WAL_SEGMENT_OFFSET))).append('\n');
    text.append("WAL Position:\t").append(String.format("%016X", getLongValue(WAL_POSITION_OFFSET))).append('\n');
    text.append("Next Page:\t\t").append(String.format("%016X", getLongValue(NEXT_PAGE_OFFSET))).append('\n');
    text.append("Prev Page:\t\t").append(String.format("%016X", getLongValue(PREV_PAGE_OFFSET))).append('\n');
    text.append("Free List:\t\t").append(String.format("%08X", getIntValue(FREELIST_HEADER_OFFSET))).append('\n');
    text.append("Free Pointer:\t").append(String.format("%08X", getIntValue(FREE_POSITION_OFFSET))).append('\n');
    text.append("Free Space:\t\t").append(String.format("%08X", getIntValue(FREE_SPACE_COUNTER_OFFSET))).append('\n');
    text.append("Entry Count:\t").append(String.format("%08X", getIntValue(ENTRIES_COUNT_OFFSET))).append('\n');
    final int indexCount = getIntValue(PAGE_INDEXES_LENGTH_OFFSET);
    text.append("Index Count:\t").append(String.format("%08X", indexCount)).append("\n\n");

    int foundEntries = 0;
    for (int i = 0; i < indexCount; ++i) {
      final int offset = getIntValue(PAGE_INDEXES_OFFSET + i * INDEX_ITEM_SIZE);
      text.append("\tOffset:\t\t").append(String.format("%08X", offset)).append(" (").append(i).append(")\n");
      text.append("\tVersion:\t")
          .append(String.format("%08X", getIntValue(PAGE_INDEXES_OFFSET + i * INDEX_ITEM_SIZE + OIntegerSerializer.INT_SIZE)))
          .append('\n');

      if ((offset & MARKED_AS_DELETED_FLAG) != 0) {
        continue;
      }

      final int cleanOffset = offset & POSITION_MASK;

      text.append("\t\tEntry Size:\t");
      if (cleanOffset + OIntegerSerializer.INT_SIZE <= MAX_PAGE_SIZE_BYTES) {
        text.append(String.format("%08X", getIntValue(cleanOffset))).append(" (").append(foundEntries).append(")\n");
      } else {
        text.append("?\n");
      }

      if (cleanOffset + OIntegerSerializer.INT_SIZE * 2 <= MAX_PAGE_SIZE_BYTES) {
        text.append("\t\tIndex:\t\t").append(String.format("%08X", getIntValue(cleanOffset + OIntegerSerializer.INT_SIZE)))
            .append('\n');
      } else {
        text.append("?\n");
      }

      if (cleanOffset + OIntegerSerializer.INT_SIZE * 3 <= MAX_PAGE_SIZE_BYTES) {
        text.append("\t\tData Size:\t").append(String.format("%08X", getIntValue(cleanOffset + OIntegerSerializer.INT_SIZE * 2)))
            .append('\n');
      } else {
        text.append("?\n");
      }

      ++foundEntries;
    }

    OLogManager.instance().error(this, "%s", null, text);
  }

  private boolean insideRecordBounds(final int entryPosition, final int offset, final int contentSize) {
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

    final int maxEntries = recordsCount /* live records */ + recordsCount + 1 /* max holes after merging */;
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

    // 2. Iterate entries in reverse, update data offsets, merge consecutive data segments and move them in a single operation.

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

      if (mergedDataSize > 0 && (entryKind == ENTRY_KIND_HOLE || i == 0)) { // move consecutive merged data segments in one go
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
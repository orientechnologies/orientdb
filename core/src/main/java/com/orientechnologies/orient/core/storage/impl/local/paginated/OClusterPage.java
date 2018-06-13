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

package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

import java.nio.ByteBuffer;

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

  static final int MAX_ENTRY_SIZE = PAGE_SIZE - PAGE_INDEXES_OFFSET - INDEX_ITEM_SIZE;

  static final int MAX_RECORD_SIZE = MAX_ENTRY_SIZE - 3 * OIntegerSerializer.INT_SIZE;

  private static final int ENTRY_KIND_HOLE    = -1;
  private static final int ENTRY_KIND_UNKNOWN = 0;
  private static final int ENTRY_KIND_DATA    = +1;

  OClusterPage(final OCacheEntry cacheEntry, final boolean newPage) {
    super(cacheEntry);

    if (newPage) {
      buffer.putLong(NEXT_PAGE_OFFSET, -1);
      buffer.putLong(PREV_PAGE_OFFSET, -1);

      buffer.putInt(FREE_POSITION_OFFSET, PAGE_SIZE);
      buffer.putInt(FREE_SPACE_COUNTER_OFFSET, PAGE_SIZE - PAGE_INDEXES_OFFSET);

      cacheEntry.markDirty();
    }
  }

  int appendRecord(final int recordVersion, final byte[] record) {
    int freePosition = buffer.getInt(FREE_POSITION_OFFSET);
    final int indexesLength = buffer.getInt(PAGE_INDEXES_LENGTH_OFFSET);

    final int lastEntryIndexPosition = PAGE_INDEXES_OFFSET + indexesLength * INDEX_ITEM_SIZE;

    final int entrySize = record.length + 3 * OIntegerSerializer.INT_SIZE;
    final int freeListHeader = buffer.getInt(FREELIST_HEADER_OFFSET);

    if (!checkSpace(entrySize, freeListHeader, buffer)) {
      return -1;
    }

    cacheEntry.markDirty();

    if (freeListHeader > 0) {
      if (freePosition - entrySize < lastEntryIndexPosition)
        doDefragmentation();
    } else {
      if (freePosition - entrySize < lastEntryIndexPosition + INDEX_ITEM_SIZE)
        doDefragmentation();
    }

    freePosition = buffer.getInt(FREE_POSITION_OFFSET);
    freePosition -= entrySize;
    final int entryIndex;

    if (freeListHeader > 0) {
      entryIndex = freeListHeader - 1;

      final int tombstonePointer = buffer.getInt(PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * entryIndex);

      final int nextEntryPosition = tombstonePointer & POSITION_MASK;
      if (nextEntryPosition > 0)
        buffer.putInt(FREELIST_HEADER_OFFSET, nextEntryPosition);
      else
        buffer.putInt(FREELIST_HEADER_OFFSET, 0);

      buffer.putInt(FREE_SPACE_COUNTER_OFFSET, getFreeSpace() - entrySize);

      final int entryIndexPosition = PAGE_INDEXES_OFFSET + entryIndex * INDEX_ITEM_SIZE;
      buffer.putInt(entryIndexPosition, freePosition);

      buffer.putInt(entryIndexPosition + OIntegerSerializer.INT_SIZE, recordVersion);
    } else {
      entryIndex = indexesLength;

      buffer.putInt(PAGE_INDEXES_LENGTH_OFFSET, indexesLength + 1);
      buffer.putInt(FREE_SPACE_COUNTER_OFFSET, getFreeSpace() - entrySize - INDEX_ITEM_SIZE);

      final int entryIndexPosition = PAGE_INDEXES_OFFSET + entryIndex * INDEX_ITEM_SIZE;
      buffer.putInt(entryIndexPosition, freePosition);

      buffer.putInt(entryIndexPosition + OIntegerSerializer.INT_SIZE, recordVersion);
    }

    buffer.position(freePosition);

    buffer.putInt(entrySize);
    buffer.putInt(entryIndex);
    buffer.putInt(record.length);
    buffer.put(record);

    buffer.putInt(FREE_POSITION_OFFSET, freePosition);

    buffer.putInt(ENTRIES_COUNT_OFFSET, buffer.getInt(ENTRIES_COUNT_OFFSET) + 1);

    return entryIndex;
  }

  int replaceRecord(final int entryIndex, final byte[] record, final int recordVersion) {
    cacheEntry.markDirty();

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + entryIndex * INDEX_ITEM_SIZE;

    if (recordVersion != -1) {
      buffer.putInt(entryIndexPosition + OIntegerSerializer.INT_SIZE, recordVersion);
    }

    final int entryPointer = buffer.getInt(entryIndexPosition);
    final int entryPosition = entryPointer & POSITION_MASK;

    final int recordSize = buffer.getInt(entryPosition) - 3 * OIntegerSerializer.INT_SIZE;
    final int writtenBytes;
    if (record.length <= recordSize) {
      buffer.putInt(entryPointer + 2 * OIntegerSerializer.INT_SIZE, record.length);

      buffer.position(entryPointer + 3 * OIntegerSerializer.INT_SIZE);
      buffer.put(record);

      writtenBytes = record.length;
    } else {
      final byte[] newRecord = new byte[recordSize];
      System.arraycopy(record, 0, newRecord, 0, newRecord.length);

      buffer.position(entryPointer + 3 * OIntegerSerializer.INT_SIZE);
      buffer.put(newRecord);

      writtenBytes = newRecord.length;
    }

    return writtenBytes;
  }

  public int getRecordVersion(final int position) {
    final int indexesLength = buffer.getInt(PAGE_INDEXES_LENGTH_OFFSET);
    if (position >= indexesLength) {
      return -1;
    }

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + position * INDEX_ITEM_SIZE;
    return buffer.getInt(entryIndexPosition + OIntegerSerializer.INT_SIZE);
  }

  public boolean isEmpty() {
    return buffer.getInt(FREE_SPACE_COUNTER_OFFSET) == PAGE_SIZE - PAGE_INDEXES_OFFSET;
  }

  private static boolean checkSpace(final int entrySize, final int freeListHeader, final ByteBuffer buffer) {
    if (freeListHeader > 0) {
      if (buffer.getInt(FREE_SPACE_COUNTER_OFFSET) - entrySize < 0) {
        return false;
      }
    } else {
      if (buffer.getInt(FREE_SPACE_COUNTER_OFFSET) - entrySize - INDEX_ITEM_SIZE < 0) {
        return false;
      }
    }
    return true;
  }

  public boolean deleteRecord(final int position) {
    final int indexesLength = buffer.getInt(PAGE_INDEXES_LENGTH_OFFSET);
    if (position >= indexesLength) {
      return false;
    }

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * position;
    final int entryPointer = buffer.getInt(entryIndexPosition);

    if ((entryPointer & MARKED_AS_DELETED_FLAG) != 0) {
      return false;
    }

    cacheEntry.markDirty();
    final int entryPosition = entryPointer & POSITION_MASK;

    final int freeListHeader = buffer.getInt(FREELIST_HEADER_OFFSET);
    if (freeListHeader <= 0) {
      buffer.putInt(entryIndexPosition, MARKED_AS_DELETED_FLAG);
    } else {
      buffer.putInt(entryIndexPosition, freeListHeader | MARKED_AS_DELETED_FLAG);
    }

    buffer.putInt(FREELIST_HEADER_OFFSET, position + 1);

    final int entrySize = buffer.getInt(entryPosition);
    assert entrySize > 0;

    buffer.putInt(entryPosition, -entrySize);
    buffer.putInt(FREE_SPACE_COUNTER_OFFSET, buffer.getInt(FREE_SPACE_COUNTER_OFFSET) + entrySize);

    buffer.putInt(ENTRIES_COUNT_OFFSET, buffer.getInt(ENTRIES_COUNT_OFFSET) - 1);

    return true;
  }

  public boolean isDeleted(final int position) {
    final int indexesLength = buffer.getInt(PAGE_INDEXES_LENGTH_OFFSET);
    if (position >= indexesLength) {
      return true;
    }

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * position;
    final int entryPointer = buffer.getInt(entryIndexPosition);

    return (entryPointer & MARKED_AS_DELETED_FLAG) != 0;
  }

  @Override
  protected byte[] serializePage() {
    final int indexesLength = buffer.getInt(PAGE_INDEXES_LENGTH_OFFSET);
    final int lastEntryIndexPosition = PAGE_INDEXES_OFFSET + indexesLength * INDEX_ITEM_SIZE;
    int size = lastEntryIndexPosition;

    final int freePosition = buffer.getInt(FREE_POSITION_OFFSET);
    final int dataSize = PAGE_SIZE - freePosition;
    size += dataSize;
    final byte[] page = new byte[size];

    buffer.position(0);
    buffer.get(page, 0, lastEntryIndexPosition);

    if (dataSize > 0) {
      buffer.position(freePosition);
      buffer.get(page, lastEntryIndexPosition, dataSize);
    }

    return page;
  }

  @Override
  protected void deserializePage(final byte[] page) {
    buffer.position(0);
    buffer.put(page, 0, PAGE_INDEXES_OFFSET);

    final int indexesLength = buffer.getInt(PAGE_INDEXES_LENGTH_OFFSET);
    final int indexesSize = indexesLength * INDEX_ITEM_SIZE;
    final int lastEntryIndexPosition = PAGE_INDEXES_OFFSET + indexesSize;
    if (indexesLength > 0) {
      buffer.put(page, PAGE_INDEXES_OFFSET, indexesSize);
    }

    final int freePosition = buffer.getInt(FREE_POSITION_OFFSET);
    final int dataSize = PAGE_SIZE - freePosition;
    if (dataSize > 0) {
      buffer.position(freePosition);
      buffer.put(page, lastEntryIndexPosition, dataSize);
    }

    cacheEntry.markDirty();
  }

  int getRecordSize(final int position) {
    final int indexesLength = buffer.getInt(PAGE_INDEXES_LENGTH_OFFSET);
    if (position >= indexesLength) {
      return -1;
    }

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * position;
    final int entryPointer = buffer.getInt(entryIndexPosition);
    if ((entryPointer & MARKED_AS_DELETED_FLAG) != 0) {
      return -1;
    }

    final int entryPosition = entryPointer & POSITION_MASK;
    return buffer.getInt(entryPosition + 2 * OIntegerSerializer.INT_SIZE);
  }

  int findFirstDeletedRecord(final int position) {
    final int indexesLength = buffer.getInt(PAGE_INDEXES_LENGTH_OFFSET);
    for (int i = position; i < indexesLength; i++) {
      final int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * i;
      final int entryPointer = buffer.getInt(entryIndexPosition);

      if ((entryPointer & MARKED_AS_DELETED_FLAG) != 0) {
        return i;
      }
    }

    return -1;
  }

  int findFirstRecord(final int position) {
    final int indexesLength = buffer.getInt(PAGE_INDEXES_LENGTH_OFFSET);
    for (int i = position; i < indexesLength; i++) {
      final int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * i;
      final int entryPointer = buffer.getInt(entryIndexPosition);

      if ((entryPointer & MARKED_AS_DELETED_FLAG) == 0) {
        return i;
      }
    }

    return -1;
  }

  int findLastRecord(final int position) {
    final int indexesLength = buffer.getInt(PAGE_INDEXES_LENGTH_OFFSET);

    final int endIndex = Math.min(indexesLength - 1, position);
    for (int i = endIndex; i >= 0; i--) {
      final int entryIndexPosition = PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * i;
      final int entryPointer = buffer.getInt(entryIndexPosition);

      if ((entryPointer & MARKED_AS_DELETED_FLAG) == 0) {
        return i;
      }
    }

    return -1;
  }

  public int getFreeSpace() {
    return buffer.getInt(FREE_SPACE_COUNTER_OFFSET);
  }

  int getMaxRecordSize() {
    final int freeListHeader = buffer.getInt(FREELIST_HEADER_OFFSET);

    final int maxEntrySize;
    if (freeListHeader > 0) {
      maxEntrySize = buffer.getInt(FREE_SPACE_COUNTER_OFFSET);
    } else {
      maxEntrySize = buffer.getInt(FREE_SPACE_COUNTER_OFFSET) - INDEX_ITEM_SIZE;
    }

    final int result = maxEntrySize - 3 * OIntegerSerializer.INT_SIZE;
    if (result < 0) {
      return 0;
    }

    return result;
  }

  int getRecordsCount() {
    return buffer.getInt(ENTRIES_COUNT_OFFSET);
  }

  long getNextPage() {
    return buffer.getInt(NEXT_PAGE_OFFSET);
  }

  void setNextPage(final long nextPage) {
    buffer.putLong(NEXT_PAGE_OFFSET, nextPage);
    cacheEntry.markDirty();
  }

  long getPrevPage() {
    return buffer.getLong(PREV_PAGE_OFFSET);
  }

  void setPrevPage(final long prevPage) {
    buffer.putLong(PREV_PAGE_OFFSET, prevPage);
    cacheEntry.markDirty();
  }

  void setNextPagePointer(final int recordPosition, final long value) {
    cacheEntry.markDirty();

    assert isPositionInsideInterval(recordPosition);

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + recordPosition * INDEX_ITEM_SIZE;
    final int entryPointer = buffer.getInt(entryIndexPosition);
    final int entryPosition = entryPointer & POSITION_MASK;

    final int recordSize = buffer.getInt(entryPosition + 2 * OIntegerSerializer.INT_SIZE);
    assert insideRecordBounds(entryPosition, recordSize - OLongSerializer.LONG_SIZE, OLongSerializer.LONG_SIZE);
    buffer.putLong(entryPosition + 3 * OIntegerSerializer.INT_SIZE + recordSize - OLongSerializer.LONG_SIZE, value);
  }

  long getRecordLongValue(final int recordPosition, final int offset) {
    assert isPositionInsideInterval(recordPosition);

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + recordPosition * INDEX_ITEM_SIZE;
    final int entryPointer = buffer.getInt(entryIndexPosition);
    final int entryPosition = entryPointer & POSITION_MASK;

    if (offset >= 0) {
      assert insideRecordBounds(entryPosition, offset, OLongSerializer.LONG_SIZE);
      return buffer.getLong(entryPosition + offset + 3 * OIntegerSerializer.INT_SIZE);
    } else {
      final int recordSize = buffer.getInt(entryPosition + 2 * OIntegerSerializer.INT_SIZE);
      assert insideRecordBounds(entryPosition, recordSize + offset, OLongSerializer.LONG_SIZE);
      return buffer.getLong(entryPosition + 3 * OIntegerSerializer.INT_SIZE + recordSize + offset);
    }
  }

  byte[] getRecordBinaryValue(final int recordPosition, final int size) {
    assert isPositionInsideInterval(recordPosition);

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + recordPosition * INDEX_ITEM_SIZE;
    final int entryPointer = buffer.getInt(entryIndexPosition);
    final int entryPosition = entryPointer & POSITION_MASK;

    assert insideRecordBounds(entryPosition, 0, size);

    final byte[] value = new byte[size];

    final ByteBuffer buffer = getBufferDuplicate();
    buffer.position(entryPosition + 3 * OIntegerSerializer.INT_SIZE);
    buffer.get(value);

    return value;
  }

  byte getRecordByteValue(final int recordPosition, final int offset) {
    assert isPositionInsideInterval(recordPosition);

    final int entryIndexPosition = PAGE_INDEXES_OFFSET + recordPosition * INDEX_ITEM_SIZE;
    final int entryPointer = buffer.getInt(entryIndexPosition);
    final int entryPosition = entryPointer & POSITION_MASK;

    if (offset >= 0) {
      assert insideRecordBounds(entryPosition, offset, OByteSerializer.BYTE_SIZE);
      return buffer.get(entryPosition + offset + 3 * OIntegerSerializer.INT_SIZE);
    } else {
      final int recordSize = buffer.getInt(entryPosition + 2 * OIntegerSerializer.INT_SIZE);
      assert insideRecordBounds(entryPosition, recordSize + offset, OByteSerializer.BYTE_SIZE);
      return buffer.get(entryPosition + 3 * OIntegerSerializer.INT_SIZE + recordSize + offset);
    }
  }

  /**
   * Writes binary dump of this cluster page to the log.
   */
  void dumpToLog() {
    final ByteBuffer buffer = getBufferDuplicate();
    final StringBuilder text = new StringBuilder();

    text.append("Dump of ").append(this).append('\n');
    text.append("Magic:\t\t\t").append(String.format("%016X", buffer.getLong(MAGIC_NUMBER_OFFSET))).append('\n');
    text.append("CRC32:\t\t\t").append(String.format("%08X", buffer.getInt(CRC32_OFFSET))).append('\n');
    text.append("WAL Segment:\t").append(String.format("%016X", buffer.getLong(WAL_SEGMENT_OFFSET))).append('\n');
    text.append("WAL Position:\t").append(String.format("%016X", buffer.getLong(WAL_POSITION_OFFSET))).append('\n');
    text.append("Next Page:\t\t").append(String.format("%016X", buffer.getLong(NEXT_PAGE_OFFSET))).append('\n');
    text.append("Prev Page:\t\t").append(String.format("%016X", buffer.getLong(PREV_PAGE_OFFSET))).append('\n');
    text.append("Free List:\t\t").append(String.format("%08X", buffer.getInt(FREELIST_HEADER_OFFSET))).append('\n');
    text.append("Free Pointer:\t").append(String.format("%08X", buffer.getInt(FREE_POSITION_OFFSET))).append('\n');
    text.append("Free Space:\t\t").append(String.format("%08X", buffer.getInt(FREE_SPACE_COUNTER_OFFSET))).append('\n');
    text.append("Entry Count:\t").append(String.format("%08X", buffer.getInt(ENTRIES_COUNT_OFFSET))).append('\n');
    final int indexCount = buffer.getInt(PAGE_INDEXES_LENGTH_OFFSET);
    text.append("Index Count:\t").append(String.format("%08X", indexCount)).append("\n\n");

    int foundEntries = 0;
    for (int i = 0; i < indexCount; ++i) {
      final int offset = buffer.getInt(PAGE_INDEXES_OFFSET + i * INDEX_ITEM_SIZE);
      text.append("\tOffset:\t\t").append(String.format("%08X", offset)).append(" (").append(i).append(")\n");
      text.append("\tVersion:\t")
          .append(String.format("%08X", buffer.getInt(PAGE_INDEXES_OFFSET + i * INDEX_ITEM_SIZE + OIntegerSerializer.INT_SIZE)))
          .append('\n');

      if ((offset & MARKED_AS_DELETED_FLAG) != 0)
        continue;

      final int cleanOffset = offset & POSITION_MASK;

      text.append("\t\tEntry Size:\t");
      if (cleanOffset + OIntegerSerializer.INT_SIZE <= MAX_PAGE_SIZE_BYTES)
        text.append(String.format("%08X", buffer.getInt(cleanOffset))).append(" (").append(foundEntries).append(")\n");
      else
        text.append("?\n");

      if (cleanOffset + OIntegerSerializer.INT_SIZE * 2 <= MAX_PAGE_SIZE_BYTES)
        text.append("\t\tIndex:\t\t").append(String.format("%08X", buffer.getInt(cleanOffset + OIntegerSerializer.INT_SIZE)))
            .append('\n');
      else
        text.append("?\n");

      if (cleanOffset + OIntegerSerializer.INT_SIZE * 3 <= MAX_PAGE_SIZE_BYTES)
        text.append("\t\tData Size:\t").append(String.format("%08X", buffer.getInt(cleanOffset + OIntegerSerializer.INT_SIZE * 2)))
            .append('\n');
      else
        text.append("?\n");

      ++foundEntries;
    }

    OLogManager.instance().error(this, "%s", null, text);
  }

  private boolean insideRecordBounds(final int entryPosition, final int offset, final int contentSize) {
    final int recordSize = buffer.getInt(entryPosition + 2 * OIntegerSerializer.INT_SIZE);
    return offset >= 0 && offset + contentSize <= recordSize;
  }

  private boolean isPositionInsideInterval(final int recordPosition) {
    final int indexesLength = buffer.getInt(PAGE_INDEXES_LENGTH_OFFSET);
    return recordPosition < indexesLength;
  }

  private void doDefragmentation() {
    final int recordsCount = buffer.getInt(ENTRIES_COUNT_OFFSET);
    final int freePosition = buffer.getInt(FREE_POSITION_OFFSET);

    // 1. Build the entries "map" and merge consecutive holes.

    final int maxEntries = recordsCount /* live records */ + recordsCount + 1 /* max holes after merging */;
    final int[] positions = new int[maxEntries];
    final int[] sizes = new int[maxEntries];

    int count = 0;
    int currentPosition = freePosition;
    int lastEntryKind = ENTRY_KIND_UNKNOWN;
    while (currentPosition < PAGE_SIZE) {
      final int size = buffer.getInt(currentPosition);
      final int entryKind = Integer.signum(size);
      assert entryKind != ENTRY_KIND_UNKNOWN;

      if (entryKind == ENTRY_KIND_HOLE && lastEntryKind == ENTRY_KIND_HOLE)
        sizes[count - 1] += size;
      else {
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
        final int positionIndex = buffer.getInt(position + OIntegerSerializer.INT_SIZE);
        buffer.putInt(PAGE_INDEXES_OFFSET + INDEX_ITEM_SIZE * positionIndex, position + shift);

        lastDataPosition = position;
        mergedDataSize += size; // accumulate consecutive data segments size
      }

      if (mergedDataSize > 0 && (entryKind == ENTRY_KIND_HOLE || i == 0)) { // move consecutive merged data segments in one go
        moveData(lastDataPosition, lastDataPosition + shift, mergedDataSize);
        mergedDataSize = 0;
      }

      if (entryKind == ENTRY_KIND_HOLE)
        shift += -size;
    }

    // 3. Update free position.

    buffer.putInt(FREE_POSITION_OFFSET, freePosition + shift);
  }

}
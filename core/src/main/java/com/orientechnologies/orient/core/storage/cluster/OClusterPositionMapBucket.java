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
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import java.util.Objects;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 10/7/13
 */
public final class OClusterPositionMapBucket extends ODurablePage {
  private static final int NEXT_PAGE_OFFSET = NEXT_FREE_POSITION;
  private static final int SIZE_OFFSET = NEXT_PAGE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int POSITIONS_OFFSET = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  // NEVER USED ON DISK
  public static final byte NOT_EXISTENT = 0;
  public static final byte REMOVED = 1;
  public static final byte FILLED = 2;
  public static final byte ALLOCATED = 4;

  private static final int ENTRY_SIZE =
      OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE;

  public static final int MAX_ENTRIES = (MAX_PAGE_SIZE_BYTES - POSITIONS_OFFSET) / ENTRY_SIZE;

  public OClusterPositionMapBucket(OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void init() {
    setIntValue(SIZE_OFFSET, 0);
  }

  public int add(long pageIndex, int recordPosition) {
    return add((int) pageIndex, recordPosition, FILLED);
  }

  public int add(int pageIndex, int recordPosition, byte status) {
    int size = getIntValue(SIZE_OFFSET);

    int position = entryPosition(size);

    position += setByteValue(position, status);
    position += setLongValue(position, pageIndex);
    setIntValue(position, recordPosition);

    setIntValue(SIZE_OFFSET, size + 1);

    return size;
  }

  public int allocate() {
    int size = getIntValue(SIZE_OFFSET);

    int position = entryPosition(size);

    position += setByteValue(position, ALLOCATED);
    position += setLongValue(position, -1);
    setIntValue(position, -1);

    setIntValue(SIZE_OFFSET, size + 1);

    return size;
  }

  public void truncateLastEntry() {
    final int size = getIntValue(SIZE_OFFSET);
    setIntValue(SIZE_OFFSET, size - 1);
  }

  public PositionEntry get(int index) {
    int size = getIntValue(SIZE_OFFSET);

    if (index >= size) {
      return null;
    }

    int position = entryPosition(index);
    if (getByteValue(position) != FILLED) {
      return null;
    }

    return readEntry(position);
  }

  public void set(final int index, final PositionEntry entry) {
    final int size = getIntValue(SIZE_OFFSET);

    if (index >= size) {
      throw new OStorageException("Provided index " + index + " is out of range");
    }

    final int position = entryPosition(index);

    byte flag = getByteValue(position);
    if (flag == ALLOCATED) {
      flag = FILLED;
    } else if (flag != FILLED) {
      throw new OStorageException("Provided index " + index + " points to removed entry");
    }

    updateEntry(index, (int) entry.pageIndex, entry.recordPosition, flag);
  }

  public void updateEntry(
      final int index, final int pageIndex, final int recordPosition, final byte status) {
    final int position = entryPosition(index);
    setByteValue(position, status);
    setLongValue(position + OByteSerializer.BYTE_SIZE, pageIndex);
    setIntValue(position + OByteSerializer.BYTE_SIZE + OLongSerializer.LONG_SIZE, recordPosition);
  }

  private static int entryPosition(int index) {
    return index * ENTRY_SIZE + POSITIONS_OFFSET;
  }

  public boolean isFull() {
    return getIntValue(SIZE_OFFSET) == MAX_ENTRIES;
  }

  public int getSize() {
    return getIntValue(SIZE_OFFSET);
  }

  public void remove(int index) {
    int size = getIntValue(SIZE_OFFSET);

    if (index >= size) {
      return;
    }

    int position = entryPosition(index);

    if (getByteValue(position) != FILLED) {
      return;
    }

    updateStatus(index, REMOVED);
  }

  public void updateStatus(int index, byte status) {
    int position = entryPosition(index);
    setByteValue(position, status);
  }

  private PositionEntry readEntry(int position) {
    position += OByteSerializer.BYTE_SIZE;
    final long pageIndex = getLongValue(position);
    position += OLongSerializer.LONG_SIZE;
    final int pagePosition = getIntValue(position);
    return new PositionEntry(pageIndex, pagePosition);
  }

  public boolean exists(final int index) {
    int size = getIntValue(SIZE_OFFSET);
    if (index >= size) {
      return false;
    }

    final int position = entryPosition(index);
    return getByteValue(position) == FILLED;
  }

  public byte getStatus(final int index) {
    int size = getIntValue(SIZE_OFFSET);
    if (index >= size) {
      return NOT_EXISTENT;
    }

    final int position = entryPosition(index);
    return getByteValue(position);
  }

  public static final class PositionEntry {
    private final long pageIndex;
    private final int recordPosition;

    public PositionEntry(final long pageIndex, final int recordPosition) {
      this.pageIndex = pageIndex;
      this.recordPosition = recordPosition;
    }

    public long getPageIndex() {
      return pageIndex;
    }

    public int getRecordPosition() {
      return recordPosition;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      PositionEntry that = (PositionEntry) o;
      return pageIndex == that.pageIndex && recordPosition == that.recordPosition;
    }

    @Override
    public int hashCode() {
      return Objects.hash(pageIndex, recordPosition);
    }

    @Override
    public String toString() {
      return "PositionEntry{"
          + "pageIndex="
          + pageIndex
          + ", recordPosition="
          + recordPosition
          + '}';
    }
  }
}

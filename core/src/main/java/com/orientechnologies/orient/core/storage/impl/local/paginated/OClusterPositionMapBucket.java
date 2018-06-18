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

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

import java.nio.ByteBuffer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 10/7/13
 */
public final class OClusterPositionMapBucket extends ODurablePage {
  private static final int NEXT_PAGE_OFFSET = NEXT_FREE_POSITION;
  private static final int SIZE_OFFSET      = NEXT_PAGE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int POSITIONS_OFFSET = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  // NEVER USED ON DISK
  static final        byte NOT_EXISTENT = 0;
  public static final byte REMOVED      = 1;
  static final        byte FILLED       = 2;
  static final        byte ALLOCATED    = 4;

  private static final int ENTRY_SIZE = OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE;

  static final int MAX_ENTRIES = (MAX_PAGE_SIZE_BYTES - POSITIONS_OFFSET) / ENTRY_SIZE;

  OClusterPositionMapBucket(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public int add(final long pageIndex, final int recordPosition) {
    final int size = buffer.getInt(SIZE_OFFSET);

    int position = entryPosition(size);

    buffer.put(position, FILLED);
    position += OByteSerializer.BYTE_SIZE;

    buffer.putLong(pageIndex);
    position += OLongSerializer.LONG_SIZE;

    buffer.putInt(position, recordPosition);

    buffer.putInt(SIZE_OFFSET, size + 1);
    cacheEntry.markDirty();

    return size;
  }

  public int allocate() {
    final int size = buffer.getInt(SIZE_OFFSET);

    final int position = entryPosition(size);

    buffer.put(position, ALLOCATED);
    buffer.putInt(SIZE_OFFSET, size + 1);

    cacheEntry.markDirty();

    return size;
  }

  public PositionEntry get(final int index) {
    final int size = buffer.getInt(SIZE_OFFSET);

    if (index >= size) {
      return null;
    }

    final int position = entryPosition(index);
    if (buffer.get(position) != FILLED) {
      return null;
    }

    return readEntry(position, buffer);
  }

  public void set(final int index, final PositionEntry entry) {
    final int size = buffer.getInt(SIZE_OFFSET);

    if (index >= size) {
      throw new OStorageException("Provided index " + index + " is out of range");
    }

    final int position = entryPosition(index);
    final byte flag = buffer.get(position);
    if (flag == ALLOCATED) {
      buffer.put(position, FILLED);
    } else if (flag != FILLED) {
      throw new OStorageException("Provided index " + index + " points to removed entry");
    }

    updateEntry(position, entry, buffer);

    cacheEntry.markDirty();
  }

  void resurrect(final int index, final PositionEntry entry) {
    final int size = buffer.getInt(SIZE_OFFSET);

    if (index >= size) {
      throw new OStorageException("Cannot resurrect a record: provided index " + index + " is out of range");
    }

    final int position = entryPosition(index);
    final byte flag = buffer.get(position);
    if (flag == REMOVED) {
      buffer.put(position, FILLED);
    } else {
      throw new OStorageException("Cannot resurrect a record: provided index " + index + " points to a non removed entry");
    }

    updateEntry(position, entry, buffer);
    cacheEntry.markDirty();
  }

  void makeAvailable(final int index) {
    final int size = buffer.getInt(SIZE_OFFSET);

    if (index >= size) {
      throw new OStorageException("Cannot make available index " + index + ", it is out of range");
    }

    final int position = entryPosition(index);
    final byte flag = buffer.get(position);

    if (flag != NOT_EXISTENT) {
      buffer.put(position, NOT_EXISTENT);
    } else {
      throw new OStorageException("Cannot make available index " + index + ", it points to a non removed entry");
    }

    if (index == size - 1) {
      buffer.putInt(SIZE_OFFSET, size - 1);
    }

    cacheEntry.markDirty();
  }

  private static int entryPosition(final int index) {
    return index * ENTRY_SIZE + POSITIONS_OFFSET;
  }

  public boolean isFull() {
    return buffer.getInt(SIZE_OFFSET) == MAX_ENTRIES;
  }

  public int getSize() {
    return buffer.getInt(SIZE_OFFSET);
  }

  public void remove(final int index) {
    final int size = buffer.getInt(SIZE_OFFSET);

    if (index >= size) {
      return;
    }

    final int position = entryPosition(index);

    if (buffer.get(position) != FILLED) {
      return;
    }

    buffer.put(position, REMOVED);
    cacheEntry.markDirty();
  }

  private static PositionEntry readEntry(int position, final ByteBuffer buffer) {
    position += OByteSerializer.BYTE_SIZE;

    final long pageIndex = buffer.getLong(position);
    position += OLongSerializer.LONG_SIZE;

    final int pagePosition = buffer.getInt(position);

    return new PositionEntry(pageIndex, pagePosition);
  }

  private static void updateEntry(int position, final PositionEntry entry, final ByteBuffer buffer) {
    position += OByteSerializer.BYTE_SIZE;

    buffer.putLong(position, entry.pageIndex);
    position += OLongSerializer.LONG_SIZE;

    buffer.putInt(position, entry.recordPosition);
  }

  public boolean exists(final int index) {
    final int size = buffer.getInt(SIZE_OFFSET);
    if (index >= size) {
      return false;
    }

    final int position = entryPosition(index);
    return buffer.get(position) == FILLED;
  }

  public byte getStatus(final int index) {
    final int size = buffer.getInt(SIZE_OFFSET);

    if (index >= size) {
      return NOT_EXISTENT;
    }

    final int position = entryPosition(index);
    return buffer.get(position);
  }

  @Override
  protected byte[] serializePage() {
    final int positions = buffer.getInt(SIZE_OFFSET);
    final int size = POSITIONS_OFFSET + positions * ENTRY_SIZE;

    final byte[] page = new byte[size];
    buffer.position(0);
    buffer.get(page);

    return page;
  }

  @Override
  protected void deserializePage(final byte[] page) {
    buffer.position(0);
    buffer.put(page);

    cacheEntry.markDirty();
  }

  @Override
  protected PageSerializationType serializationType() {
    return PageSerializationType.CLUSTER_POSITION_MAP_BUCKET;
  }

  public static class PositionEntry {
    private final long pageIndex;
    private final int  recordPosition;

    PositionEntry(final long pageIndex, final int recordPosition) {
      this.pageIndex = pageIndex;
      this.recordPosition = recordPosition;
    }

    public long getPageIndex() {
      return pageIndex;
    }

    int getRecordPosition() {
      return recordPosition;
    }

  }
}

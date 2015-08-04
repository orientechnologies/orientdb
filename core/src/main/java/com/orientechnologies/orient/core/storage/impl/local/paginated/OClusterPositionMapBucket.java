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

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 10/7/13
 */
public class OClusterPositionMapBucket extends ODurablePage {
  private static final int  NEXT_PAGE_OFFSET = NEXT_FREE_POSITION;
  private static final int  SIZE_OFFSET      = NEXT_PAGE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int  POSITIONS_OFFSET = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  private static final byte REMOVED          = 1;
  private static final byte FILLED           = 2;

  public static final int   ENTRY_SIZE       = OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE;

  public static final int   MAX_ENTRIES      = (MAX_PAGE_SIZE_BYTES - POSITIONS_OFFSET) / ENTRY_SIZE;

  public OClusterPositionMapBucket(OCacheEntry cacheEntry, OWALChangesTree changesTree) {
    super(cacheEntry, changesTree);
  }

  public int add(long pageIndex, int recordPosition) throws IOException {
    int size = getIntValue(SIZE_OFFSET);

    int position = entryPosition(size);

    position += setByteValue(position, FILLED);
    position += setLongValue(position, pageIndex);
    position += setIntValue(position, recordPosition);

    setIntValue(SIZE_OFFSET, size + 1);

    return size;
  }

  public PositionEntry get(int index) {
    int size = getIntValue(SIZE_OFFSET);

    if (index >= size)
      return null;

    int position = entryPosition(index);
    if (getByteValue(position) != FILLED)
      return null;

    return readEntry(position);
  }

  public void set(int index, PositionEntry entry) throws IOException {
    int size = getIntValue(SIZE_OFFSET);

    if (index >= size)
      throw new OStorageException("Provided index " + index + " is out of range.");

    final int position = entryPosition(index);
    if (getByteValue(position) != FILLED)
      throw new OStorageException("Provided index " + index + " points to removed entry.");

    updateEntry(position, entry);
  }

  private int entryPosition(int index) {
    return index * ENTRY_SIZE + POSITIONS_OFFSET;
  }

  public boolean isFull() {
    return getIntValue(SIZE_OFFSET) == MAX_ENTRIES;
  }

  public int getSize() {
    return getIntValue(SIZE_OFFSET);
  }

  public PositionEntry remove(int index) {
    int size = getIntValue(SIZE_OFFSET);

    if (index >= size)
      return null;

    int position = entryPosition(index);

    if (getByteValue(position) != FILLED)
      return null;

    setByteValue(position, REMOVED);

    return readEntry(position);
  }

  private PositionEntry readEntry(int position) {
    position += OByteSerializer.BYTE_SIZE;

    long pageIndex = getLongValue(position);
    position += OLongSerializer.LONG_SIZE;

    int pagePosition = getIntValue(position);
    position += OIntegerSerializer.INT_SIZE;

    return new PositionEntry(pageIndex, pagePosition);
  }

  private void updateEntry(int position, PositionEntry entry) throws IOException {
    position += OByteSerializer.BYTE_SIZE;

    setLongValue(position, entry.pageIndex);
    position += OLongSerializer.LONG_SIZE;

    setIntValue(position, entry.recordPosition);
  }

  public boolean exists(int index) {
    int size = getIntValue(SIZE_OFFSET);
    if (index >= size)
      return false;

    final int position = entryPosition(index);
    return getByteValue(position) == FILLED;
  }

  public static class PositionEntry {
    private final long pageIndex;
    private final int  recordPosition;

    public PositionEntry(long pageIndex, int recordPosition) {
      this.pageIndex = pageIndex;
      this.recordPosition = recordPosition;
    }

    public long getPageIndex() {
      return pageIndex;
    }

    public int getRecordPosition() {
      return recordPosition;
    }

  }
}

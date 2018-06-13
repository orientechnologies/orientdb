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

package com.orientechnologies.orient.core.storage.index.sbtree.local;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

import java.nio.ByteBuffer;

/**
 * This page will contain value if it exceeds value limit for SBTree. Value is stored as list of linked pages. Following format is
 * used.
 * <ol>
 * <li>Next free list page index, or -1 if page is filled by value. 8 bytes.</li>
 * <li>Whole value size. 4 bytes.</li>
 * <li>Size for current page - 4 bytes.</li>
 * <li>Next page which contains next portion of data. 8 bytes.</li>
 * <li>Serialized value presentation.</li>
 * </ol>
 * !!! This functionality should be removed after new sbtree based ridbag will be implemented, because it doest not make any sense
 * to keep it, it will provide performance degradation only !!!!!!
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 9/27/13
 */
public final class OSBTreeValuePage extends ODurablePage {
  private static final int FREE_LIST_NEXT_PAGE_OFFSET = NEXT_FREE_POSITION;
  private static final int WHOLE_VALUE_SIZE_OFFSET    = FREE_LIST_NEXT_PAGE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int PAGE_VALUE_SIZE_OFFSET     = WHOLE_VALUE_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int NEXT_VALUE_PAGE_OFFSET     = PAGE_VALUE_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int BINARY_CONTENT_OFFSET      = NEXT_VALUE_PAGE_OFFSET + OLongSerializer.LONG_SIZE;

  static final int MAX_BINARY_VALUE_SIZE = MAX_PAGE_SIZE_BYTES - BINARY_CONTENT_OFFSET;

  OSBTreeValuePage(final OCacheEntry cacheEntry, final boolean isNew) {
    super(cacheEntry);

    if (isNew) {
      setNextFreeListPage(-1);
      setNextPage(-1);
    }

  }

  void setNextPage(final long nextPage) {
    buffer.putLong(NEXT_VALUE_PAGE_OFFSET, nextPage);
    cacheEntry.markDirty();
  }

  public int getSize() {
    return buffer.getInt(WHOLE_VALUE_SIZE_OFFSET);
  }

  int fillBinaryContent(final byte[] data, final int offset) {
    buffer.putInt(WHOLE_VALUE_SIZE_OFFSET, data.length);

    final int maxSize = Math.min(data.length - offset, MAX_BINARY_VALUE_SIZE);

    buffer.putInt(PAGE_VALUE_SIZE_OFFSET, maxSize);

    final byte[] pageValue = new byte[maxSize];
    System.arraycopy(data, offset, pageValue, 0, maxSize);

    buffer.position(BINARY_CONTENT_OFFSET);
    buffer.put(pageValue);

    cacheEntry.markDirty();
    return offset + maxSize;
  }

  int readBinaryContent(final byte[] data, final int offset) {
    final int valueSize = buffer.getInt(PAGE_VALUE_SIZE_OFFSET);

    final ByteBuffer buffer = getBufferDuplicate();
    final byte[] content = new byte[valueSize];
    buffer.position(BINARY_CONTENT_OFFSET);
    buffer.get(content);

    System.arraycopy(content, 0, data, offset, valueSize);

    return offset + valueSize;
  }

  long getNextPage() {
    return buffer.getLong(NEXT_VALUE_PAGE_OFFSET);
  }

  void setNextFreeListPage(final long pageIndex) {
    buffer.putLong(FREE_LIST_NEXT_PAGE_OFFSET, pageIndex);
    cacheEntry.markDirty();
  }

  long getNextFreeListPage() {
    return buffer.getLong(FREE_LIST_NEXT_PAGE_OFFSET);
  }
}

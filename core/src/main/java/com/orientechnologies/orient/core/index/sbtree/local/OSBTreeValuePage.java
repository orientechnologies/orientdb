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

package com.orientechnologies.orient.core.index.sbtree.local;

import java.io.IOException;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

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
 * 
 * !!! This functionality should be removed after new sbtree based ridbag will be implemented, because it doest not make any sense
 * to keep it, it will provide performance degradation only !!!!!!
 * 
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 9/27/13
 */
public class OSBTreeValuePage extends ODurablePage {
  private static final int FREE_LIST_NEXT_PAGE_OFFSET = NEXT_FREE_POSITION;
  private static final int WHOLE_VALUE_SIZE_OFFSET    = FREE_LIST_NEXT_PAGE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int PAGE_VALUE_SIZE_OFFSET     = WHOLE_VALUE_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int NEXT_VALUE_PAGE_OFFSET     = PAGE_VALUE_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int BINARY_CONTENT_OFFSET      = NEXT_VALUE_PAGE_OFFSET + OLongSerializer.LONG_SIZE;

  public static final int  MAX_BINARY_VALUE_SIZE      = MAX_PAGE_SIZE_BYTES - BINARY_CONTENT_OFFSET;

  public OSBTreeValuePage(OCacheEntry cacheEntry, boolean isNew) throws IOException {
    super(cacheEntry);

    if (isNew) {
      setNextFreeListPage(-1);
      setNextPage(-1);
    }

  }

  public void setNextPage(long nextPage) throws IOException {
    setLongValue(NEXT_VALUE_PAGE_OFFSET, nextPage);
  }

  public int getSize() {
    return getIntValue(WHOLE_VALUE_SIZE_OFFSET);
  }

  public int fillBinaryContent(byte[] data, int offset) throws IOException {
    setIntValue(WHOLE_VALUE_SIZE_OFFSET, data.length);

    int maxSize = Math.min(data.length - offset, MAX_BINARY_VALUE_SIZE);

    setIntValue(PAGE_VALUE_SIZE_OFFSET, maxSize);

    byte[] pageValue = new byte[maxSize];
    System.arraycopy(data, offset, pageValue, 0, maxSize);

    setBinaryValue(BINARY_CONTENT_OFFSET, pageValue);

    return offset + maxSize;
  }

  public int readBinaryContent(byte[] data, int offset) throws IOException {
    int valueSize = getIntValue(PAGE_VALUE_SIZE_OFFSET);
    byte[] content = getBinaryValue(BINARY_CONTENT_OFFSET, valueSize);

    System.arraycopy(content, 0, data, offset, valueSize);

    return offset + valueSize;
  }

  public long getNextPage() {
    return getLongValue(NEXT_VALUE_PAGE_OFFSET);
  }

  public void setNextFreeListPage(long pageIndex) throws IOException {
    setLongValue(FREE_LIST_NEXT_PAGE_OFFSET, pageIndex);
  }

  public long getNextFreeListPage() {
    return getLongValue(FREE_LIST_NEXT_PAGE_OFFSET);
  }

  public static int calculateAmountOfPage(int contentSize) {
    return (int) Math.ceil(1.0 * contentSize / MAX_BINARY_VALUE_SIZE);
  }
}

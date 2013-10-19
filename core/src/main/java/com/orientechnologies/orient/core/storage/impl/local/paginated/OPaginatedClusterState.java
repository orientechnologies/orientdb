/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated;

import java.io.IOException;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author Andrey Lomakin
 * @since 20.08.13
 */
public class OPaginatedClusterState extends ODurablePage {
  private static final int RECORDS_SIZE_OFFSET = NEXT_FREE_POSITION;
  private static final int SIZE_OFFSET         = RECORDS_SIZE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int FREE_LIST_OFFSET    = SIZE_OFFSET + OLongSerializer.LONG_SIZE;

  public OPaginatedClusterState(ODirectMemoryPointer pagePointer, TrackMode trackMode) {
    super(pagePointer, trackMode);
  }

  public void setSize(long size) throws IOException {
    setLongValue(SIZE_OFFSET, size);
  }

  public long getSize() {
    return getLongValue(SIZE_OFFSET);
  }

  public void setRecordsSize(long recordsSize) throws IOException {
    setLongValue(RECORDS_SIZE_OFFSET, recordsSize);
  }

  public long getRecordsSize() {
    return getLongValue(RECORDS_SIZE_OFFSET);
  }

  public void setFreeListPage(int index, long pageIndex) throws IOException {
    setLongValue(FREE_LIST_OFFSET + index * OLongSerializer.LONG_SIZE, pageIndex);
  }

  public long getFreeListPage(int index) {
    return getLongValue(FREE_LIST_OFFSET + index * OLongSerializer.LONG_SIZE);
  }
}

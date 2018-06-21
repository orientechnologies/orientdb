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

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

import java.nio.ByteBuffer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 20.08.13
 */
public final class OPaginatedClusterState extends ODurablePage {
  private static final int RECORDS_SIZE_OFFSET = NEXT_FREE_POSITION;
  private static final int SIZE_OFFSET         = RECORDS_SIZE_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int FREE_LIST_OFFSET    = SIZE_OFFSET + OLongSerializer.LONG_SIZE;

  OPaginatedClusterState(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void setSize(final long size) {
    buffer.putLong(SIZE_OFFSET, size);
    cacheEntry.markDirty();
  }

  public long getSize() {
    return buffer.getLong(SIZE_OFFSET);
  }

  void setRecordsSize(final long recordsSize) {
    buffer.putLong(RECORDS_SIZE_OFFSET, recordsSize);
    cacheEntry.markDirty();
  }

  public long getRecordsSize() {
    return buffer.getLong(RECORDS_SIZE_OFFSET);
  }

  void setFreeListPage(final int index, final long pageIndex) {
    buffer.putLong(FREE_LIST_OFFSET + index * OLongSerializer.LONG_SIZE, pageIndex);
    cacheEntry.markDirty();
  }

  long getFreeListPage(final int index) {
    return buffer.getLong(FREE_LIST_OFFSET + index * OLongSerializer.LONG_SIZE);
  }

  @Override
  public int serializedSize() {
    return FREE_LIST_OFFSET + OPaginatedCluster.FREE_LIST_SIZE * OLongSerializer.LONG_SIZE; // size of all free list elements
  }

  @Override
  public void serializePage(ByteBuffer recordBuffer) {
    assert buffer.limit() == buffer.capacity();

    final int size =
        FREE_LIST_OFFSET + OPaginatedCluster.FREE_LIST_SIZE * OLongSerializer.LONG_SIZE; // size of all free list elements

    this.buffer.position(0);
    this.buffer.limit(size);
    recordBuffer.put(this.buffer);
    this.buffer.limit(this.buffer.capacity());
  }

  @Override
  public void deserializePage(final byte[] page) {
    assert buffer.limit() == buffer.capacity();

    buffer.position(0);
    buffer.put(page);

    cacheEntry.markDirty();
  }

  @Override
  protected PageSerializationType serializationType() {
    return PageSerializationType.PAGINATED_CLUSTER_STATE;
  }
}

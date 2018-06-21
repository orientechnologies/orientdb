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

package com.orientechnologies.orient.core.storage.impl.local.paginated.base;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.PageSerializationType;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Base page class for all durable data structures, that is data structures state of which can be consistently restored after system
 * crash but results of last operations in small interval before crash may be lost.
 * This page has several booked memory areas with following offsets at the beginning:
 * <ol>
 * <li>from 0 to 7 - Magic number</li>
 * <li>from 8 to 11 - crc32 of all page content, which is calculated by cache system just before save</li>
 * <li>from 12 to 23 - LSN of last operation which was stored for given page</li>
 * </ol>
 * Developer which will extend this class should use all page memory starting from {@link #NEXT_FREE_POSITION} offset.
 * All data structures which use this kind of pages should be derived from
 * {@link com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent} class.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 16.08.13
 */
public class ODurablePage {

  public static final    int MAGIC_NUMBER_OFFSET = 0;
  protected static final int CRC32_OFFSET        = MAGIC_NUMBER_OFFSET + OLongSerializer.LONG_SIZE;

  public static final int WAL_SEGMENT_OFFSET  = CRC32_OFFSET + OIntegerSerializer.INT_SIZE;
  public static final int WAL_POSITION_OFFSET = WAL_SEGMENT_OFFSET + OLongSerializer.LONG_SIZE;
  public static final int PAGE_SIZE           = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  public static final int NEXT_FREE_POSITION = WAL_POSITION_OFFSET + OLongSerializer.LONG_SIZE;

  protected final OCacheEntry cacheEntry;
  protected final ByteBuffer  buffer;

  public ODurablePage(final OCacheEntry cacheEntry) {
    assert cacheEntry != null;

    this.cacheEntry = cacheEntry;
    final OCachePointer pointer = cacheEntry.getCachePointer();
    this.buffer = pointer.getBuffer();

    assert buffer != null;
  }

  public static OLogSequenceNumber getLogSequenceNumberFromPage(final ByteBuffer buffer) {
    buffer.position(WAL_SEGMENT_OFFSET);
    final long segment = buffer.getLong();
    final long position = buffer.getLong();

    return new OLogSequenceNumber(segment, position);
  }

  static void setLogSequenceNumber(final ByteBuffer buffer, final OLogSequenceNumber lsn) {
    buffer.position(WAL_SEGMENT_OFFSET);
    buffer.putLong(lsn.getSegment());
    buffer.putLong(lsn.getPosition());
  }

  /**
   * DO NOT DELETE THIS METHOD IT IS USED IN ENTERPRISE STORAGE
   * Copies content of page into passed in byte array.
   *
   * @param buffer Buffer from which data will be copied
   * @param data   Byte array to which data will be copied
   * @param offset Offset of data inside page
   * @param length Length of data to be copied
   */
  @SuppressWarnings("unused")
  public static void getPageData(final ByteBuffer buffer, final byte[] data, final int offset, final int length) {
    buffer.position(0);
    buffer.get(data, offset, length);
  }

  /**
   * DO NOT DELETE THIS METHOD IT IS USED IN ENTERPRISE STORAGE
   * Get value of LSN from the passed in offset in byte array.
   *
   * @param offset Offset inside of byte array from which LSN value will be read.
   * @param data   Byte array from which LSN value will be read.
   */
  @SuppressWarnings("unused")
  public static OLogSequenceNumber getLogSequenceNumber(final int offset, final byte[] data) {
    final long segment = OLongSerializer.INSTANCE.deserializeNative(data, offset + WAL_SEGMENT_OFFSET);
    final long position = OLongSerializer.INSTANCE.deserializeNative(data, offset + WAL_POSITION_OFFSET);

    return new OLogSequenceNumber(segment, position);
  }

  public OCacheEntry getCacheEntry() {
    return cacheEntry;
  }

  protected final void moveData(final int from, final int to, final int len) {
    assert cacheEntry.getCachePointer().getBuffer() == null || cacheEntry.isLockAcquiredByCurrentThread();

    if (len == 0) {
      return;
    }

    final ByteBuffer rb = buffer.asReadOnlyBuffer();
    rb.position(from);
    rb.limit(from + len);

    buffer.position(to);
    buffer.put(rb);
  }

  public final void setLsn(final OLogSequenceNumber lsn) {
    assert cacheEntry.getCachePointer().getBuffer() == null || cacheEntry.isLockAcquiredByCurrentThread();

    buffer.position(WAL_SEGMENT_OFFSET);

    buffer.putLong(lsn.getSegment());
    buffer.putLong(lsn.getPosition());

    cacheEntry.markDirty();
  }

  protected final ByteBuffer getBufferDuplicate() {
    return buffer.duplicate().order(ByteOrder.nativeOrder());
  }

  public void serializePage(final ByteBuffer recordBuffer) {
    assert buffer.limit() == buffer.capacity();

    buffer.position(0);
    recordBuffer.put(buffer);
  }

  public void deserializePage(final byte[] page) {
    assert buffer.limit() == buffer.capacity();

    buffer.position(0);
    buffer.put(page);

    cacheEntry.markDirty();
  }

  public int serializedSize() {
    return buffer.capacity();
  }

  protected PageSerializationType serializationType() {
    return PageSerializationType.GENERIC;
  }

  @Override
  public String toString() {
    if (cacheEntry != null)
      return getClass().getSimpleName() + "{" + "fileId=" + cacheEntry.getFileId() + ", pageIndex=" + cacheEntry.getPageIndex()
          + '}';
    else
      return super.toString();
  }
}

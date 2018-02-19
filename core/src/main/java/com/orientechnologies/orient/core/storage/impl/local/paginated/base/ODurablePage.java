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

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;

import java.nio.ByteBuffer;

/**
 * Base page class for all durable data structures, that is data structures state of which can be consistently restored after system
 * crash but results of last operations in small interval before crash may be lost.
 * <p>
 * This page has several booked memory areas with following offsets at the beginning:
 * <ol>
 * <li>from 0 to 7 - Magic number</li>
 * <li>from 8 to 11 - crc32 of all page content, which is calculated by cache system just before save</li>
 * <li>from 12 to 23 - LSN of last operation which was stored for given page</li>
 * </ol>
 * <p>
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
  public static final int MAX_PAGE_SIZE_BYTES = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  public static final int NEXT_FREE_POSITION = WAL_POSITION_OFFSET + OLongSerializer.LONG_SIZE;

  private OWALChanges changes;

  private final OCacheEntry cacheEntry;

  private final OCachePointer pointer;

  public ODurablePage(OCacheEntry cacheEntry) {
    assert cacheEntry != null || changes != null;

    this.cacheEntry = cacheEntry;

    if (cacheEntry != null) {
      this.pointer = cacheEntry.getCachePointer();
      this.changes = cacheEntry.getChanges();
    } else
      this.pointer = null;

  }

  public static OLogSequenceNumber getLogSequenceNumberFromPage(ByteBuffer buffer) {
    buffer.position(WAL_SEGMENT_OFFSET);
    final long segment = buffer.getLong();
    final long position = buffer.getLong();

    return new OLogSequenceNumber(segment, position);
  }

  /**
   * DO NOT DELETE THIS METHOD IT IS USED IN ENTERPRISE STORAGE
   * <p>
   * Copies content of page into passed in byte array.
   *
   * @param buffer Buffer from which data will be copied
   * @param data   Byte array to which data will be copied
   * @param offset Offset of data inside page
   * @param length Length of data to be copied
   */
  @SuppressWarnings("unused")
  public static void getPageData(ByteBuffer buffer, byte[] data, int offset, int length) {
    buffer.position(0);
    buffer.get(data, offset, length);
  }

  /**
   * DO NOT DELETE THIS METHOD IT IS USED IN ENTERPRISE STORAGE
   * <p>
   * Get value of LSN from the passed in offset in byte array.
   *
   * @param offset Offset inside of byte array from which LSN value will be read.
   * @param data   Byte array from which LSN value will be read.
   */
  @SuppressWarnings("unused")
  public static OLogSequenceNumber getLogSequenceNumber(int offset, byte[] data) {
    final long segment = OLongSerializer.INSTANCE.deserializeNative(data, offset + WAL_SEGMENT_OFFSET);
    final long position = OLongSerializer.INSTANCE.deserializeNative(data, offset + WAL_POSITION_OFFSET);

    return new OLogSequenceNumber(segment, position);
  }

  protected int getIntValue(int pageOffset) {
    assert cacheEntry.getCachePointer().getBuffer() == null || cacheEntry.isLockAcquiredByCurrentThread();


    if (changes == null) {
      final ByteBuffer buffer = pointer.getBuffer();
      return buffer.getInt(pageOffset);
    }

    return changes.getIntValue(pointer.getBufferDuplicate(), pageOffset);
  }

  protected long getLongValue(int pageOffset) {
    assert cacheEntry.getCachePointer().getBuffer() == null || cacheEntry.isLockAcquiredByCurrentThread();


    if (changes == null) {
      final ByteBuffer buffer = pointer.getBuffer();
      return buffer.getLong(pageOffset);
    }

    return changes.getLongValue(pointer.getBufferDuplicate(), pageOffset);
  }

  protected byte[] getBinaryValue(int pageOffset, int valLen) {
    assert cacheEntry.getCachePointer().getBuffer() == null || cacheEntry.isLockAcquiredByCurrentThread();

    final ByteBuffer buffer = pointer.getBufferDuplicate();
    if (changes == null) {
      final byte[] result = new byte[valLen];

      buffer.position(pageOffset);
      buffer.get(result);

      return result;
    }

    return changes.getBinaryValue(buffer, pageOffset, valLen);
  }

  protected int getObjectSizeInDirectMemory(OBinarySerializer binarySerializer, int offset) {
    assert cacheEntry.getCachePointer().getBuffer() == null || cacheEntry.isLockAcquiredByCurrentThread();

    final ByteBuffer buffer = pointer.getBufferDuplicate();
    if (changes == null) {
      buffer.position(offset);
      return binarySerializer.getObjectSizeInByteBuffer(buffer);
    }

    return binarySerializer.getObjectSizeInByteBuffer(buffer, changes, offset);
  }

  protected <T> T deserializeFromDirectMemory(OBinarySerializer<T> binarySerializer, int offset) {
    assert cacheEntry.getCachePointer().getBuffer() == null || cacheEntry.isLockAcquiredByCurrentThread();

    final ByteBuffer buffer = pointer.getBufferDuplicate();
    if (changes == null) {
      buffer.position(offset);
      return binarySerializer.deserializeFromByteBufferObject(buffer);
    }

    return binarySerializer.deserializeFromByteBufferObject(buffer, changes, offset);
  }

  protected byte getByteValue(int pageOffset) {
    assert cacheEntry.getCachePointer().getBuffer() == null || cacheEntry.isLockAcquiredByCurrentThread();

    if (changes == null) {
      final ByteBuffer buffer = pointer.getBuffer();
      return buffer.get(pageOffset);
    }

    return changes.getByteValue(pointer.getBufferDuplicate(), pageOffset);
  }

  @SuppressWarnings("SameReturnValue")
  protected int setIntValue(int pageOffset, int value) {
    assert cacheEntry.getCachePointer().getBuffer() == null || cacheEntry.isLockAcquiredByCurrentThread();

    final ByteBuffer buffer = pointer.getBuffer();
    if (changes != null) {
      changes.setIntValue(buffer, value, pageOffset);
    } else {
      buffer.putInt(pageOffset, value);
      cacheEntry.markDirty();
    }

    return OIntegerSerializer.INT_SIZE;
  }

  @SuppressWarnings("SameReturnValue")
  protected int setByteValue(int pageOffset, byte value) {
    assert cacheEntry.getCachePointer().getBuffer() == null || cacheEntry.isLockAcquiredByCurrentThread();

    final ByteBuffer buffer = pointer.getBuffer();
    if (changes != null) {
      changes.setByteValue(buffer, value, pageOffset);
    } else {
      buffer.put(pageOffset, value);
      cacheEntry.markDirty();
    }

    return OByteSerializer.BYTE_SIZE;
  }

  @SuppressWarnings("SameReturnValue")
  protected int setLongValue(int pageOffset, long value) {
    assert cacheEntry.getCachePointer().getBuffer() == null || cacheEntry.isLockAcquiredByCurrentThread();

    final ByteBuffer buffer = pointer.getBuffer();
    if (changes != null) {
      changes.setLongValue(buffer, value, pageOffset);
    } else {
      buffer.putLong(pageOffset, value);
      cacheEntry.markDirty();
    }

    return OLongSerializer.LONG_SIZE;
  }

  protected int setBinaryValue(int pageOffset, byte[] value) {
    assert cacheEntry.getCachePointer().getBuffer() == null || cacheEntry.isLockAcquiredByCurrentThread();

    if (value.length == 0)
      return 0;

    final ByteBuffer buffer = pointer.getBuffer();
    if (changes != null) {
      changes.setBinaryValue(buffer, value, pageOffset);
    } else {
      buffer.position(pageOffset);
      buffer.put(value);
      cacheEntry.markDirty();
    }

    return value.length;
  }

  protected void moveData(int from, int to, int len) {
    assert cacheEntry.getCachePointer().getBuffer() == null || cacheEntry.isLockAcquiredByCurrentThread();

    if (len == 0)
      return;

    final ByteBuffer buffer = pointer.getBuffer();
    if (changes != null) {
      changes.moveData(buffer, from, to, len);
    } else {
      final ByteBuffer rb = buffer.asReadOnlyBuffer();
      rb.position(from);
      rb.limit(from + len);

      buffer.position(to);
      buffer.put(rb);

      cacheEntry.markDirty();
    }
  }

  public OWALChanges getChanges() {
    return changes;
  }

  public void restoreChanges(OWALChanges changes) {
    assert cacheEntry.getCachePointer().getBuffer() == null || cacheEntry.isLockAcquiredByCurrentThread();

    final ByteBuffer buffer = cacheEntry.getCachePointer().getBuffer();

    buffer.position(0);
    changes.applyChanges(buffer);

    cacheEntry.markDirty();
  }

  public void rollbackChanges(OWALChanges changes) {
    assert cacheEntry.getCachePointer().getBuffer() == null || cacheEntry.isLockAcquiredByCurrentThread();

    final ByteBuffer buffer = cacheEntry.getCachePointer().getBuffer();

    buffer.position(0);
    changes.applyOriginalValues(buffer);

    cacheEntry.markDirty();
  }

  public void setLsn(OLogSequenceNumber lsn) {
    assert cacheEntry.getCachePointer().getBuffer() == null || cacheEntry.isLockAcquiredByCurrentThread();

    final ByteBuffer buffer = pointer.getBuffer();
    buffer.position(WAL_SEGMENT_OFFSET);

    buffer.putLong(lsn.getSegment());
    buffer.putLong(lsn.getPosition());

    cacheEntry.markDirty();
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

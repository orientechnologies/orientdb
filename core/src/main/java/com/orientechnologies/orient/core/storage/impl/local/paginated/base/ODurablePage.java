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

import com.orientechnologies.common.serialization.types.*;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Base page class for all durable data structures, that is data structures state of which can be
 * consistently restored after system crash but results of last operations in small interval before
 * crash may be lost.
 *
 * <p>This page has several booked memory areas with following offsets at the beginning:
 *
 * <ol>
 *   <li>from 0 to 7 - Magic number
 *   <li>from 8 to 11 - crc32 of all page content, which is calculated by cache system just before
 *       save
 *   <li>from 12 to 23 - LSN of last operation which was stored for given page
 * </ol>
 *
 * <p>Developer which will extend this class should use all page memory starting from {@link
 * #NEXT_FREE_POSITION} offset. All data structures which use this kind of pages should be derived
 * from {@link
 * com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent} class.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 16.08.13
 */
public class ODurablePage {

  public static final int MAGIC_NUMBER_OFFSET = 0;
  protected static final int CRC32_OFFSET = MAGIC_NUMBER_OFFSET + OLongSerializer.LONG_SIZE;

  public static final int WAL_SEGMENT_OFFSET = CRC32_OFFSET + OIntegerSerializer.INT_SIZE;
  public static final int WAL_POSITION_OFFSET = WAL_SEGMENT_OFFSET + OLongSerializer.LONG_SIZE;

  public static final int MAX_PAGE_SIZE_BYTES =
      OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  public static final int NEXT_FREE_POSITION = WAL_POSITION_OFFSET + OLongSerializer.LONG_SIZE;

  private final OWALChanges changes;
  private final OCacheEntry cacheEntry;
  private final OCachePointer pointer;
  private final ByteBuffer buffer;

  public ODurablePage(final OCacheEntry cacheEntry) {
    assert cacheEntry != null;
    this.cacheEntry = cacheEntry;
    this.pointer = cacheEntry.getCachePointer();
    this.changes = cacheEntry.getChanges();
    this.buffer = pointer.getBufferDuplicate();

    if (cacheEntry.getInitialLSN() == null) {
      final ByteBuffer buffer = pointer.getBuffer();

      if (buffer != null) {
        cacheEntry.setInitialLSN(getLogSequenceNumberFromPage(buffer));
      } else {
        // it is new a page
        cacheEntry.setInitialLSN(new OLogSequenceNumber(0, 0));
      }
    }
  }

  public final int getPageIndex() {
    return cacheEntry.getPageIndex();
  }

  public final OLogSequenceNumber getLSN() {
    final long segment = getLongValue(WAL_SEGMENT_OFFSET);
    final int position = getIntValue(WAL_POSITION_OFFSET);

    return new OLogSequenceNumber(segment, position);
  }

  public static OLogSequenceNumber getLogSequenceNumberFromPage(final ByteBuffer buffer) {
    final long segment = buffer.getLong(WAL_SEGMENT_OFFSET);
    final int position = buffer.getInt(WAL_POSITION_OFFSET);

    return new OLogSequenceNumber(segment, position);
  }

  /**
   * DO NOT DELETE THIS METHOD IT IS USED IN ENTERPRISE STORAGE
   *
   * <p>Copies content of page into passed in byte array.
   *
   * @param buffer Buffer from which data will be copied
   * @param data Byte array to which data will be copied
   * @param offset Offset of data inside page
   * @param length Length of data to be copied
   */
  @SuppressWarnings("unused")
  public static void getPageData(
      final ByteBuffer buffer, final byte[] data, final int offset, final int length) {
    buffer.position(0);
    buffer.get(data, offset, length);
  }

  /**
   * DO NOT DELETE THIS METHOD IT IS USED IN ENTERPRISE STORAGE
   *
   * <p>Get value of LSN from the passed in offset in byte array.
   *
   * @param offset Offset inside of byte array from which LSN value will be read.
   * @param data Byte array from which LSN value will be read.
   */
  @SuppressWarnings("unused")
  public static OLogSequenceNumber getLogSequenceNumber(final int offset, final byte[] data) {
    final long segment =
        OLongSerializer.INSTANCE.deserializeNative(data, offset + WAL_SEGMENT_OFFSET);
    final int position =
        OIntegerSerializer.INSTANCE.deserializeNative(data, offset + WAL_POSITION_OFFSET);

    return new OLogSequenceNumber(segment, position);
  }

  protected final int getIntValue(final int pageOffset) {
    if (changes == null) {

      assert buffer != null;

      assert buffer.order() == ByteOrder.nativeOrder();
      return buffer.getInt(pageOffset);
    }

    return changes.getIntValue(buffer, pageOffset);
  }

  protected final int[] getIntArray(final int pageOffset, int size) {
    int[] values = new int[size];
    byte[] bytes = getBinaryValue(pageOffset, size * OIntegerSerializer.INT_SIZE);
    for (int i = 0; i < size; i++) {
      values[i] =
          OIntegerSerializer.INSTANCE.deserializeNative(bytes, i * OIntegerSerializer.INT_SIZE);
    }
    return values;
  }

  protected void setIntArray(final int pageOffset, int[] values, int offset) {
    byte[] bytes = new byte[(values.length - offset) * OIntegerSerializer.INT_SIZE];
    for (int i = offset; i < values.length; i++) {
      OIntegerSerializer.INSTANCE.serializeNative(
          values[i], bytes, (i - offset) * OIntegerSerializer.INT_SIZE);
    }
    setBinaryValue(pageOffset, bytes);
  }

  protected short getShortValue(final int pageOffset) {
    if (changes == null) {
      assert buffer != null;

      assert buffer.order() == ByteOrder.nativeOrder();
      return buffer.getShort(pageOffset);
    }

    return changes.getShortValue(buffer, pageOffset);
  }

  protected final long getLongValue(final int pageOffset) {
    if (changes == null) {
      assert buffer != null;

      assert buffer.order() == ByteOrder.nativeOrder();
      return buffer.getLong(pageOffset);
    }

    return changes.getLongValue(buffer, pageOffset);
  }

  protected final byte[] getBinaryValue(final int pageOffset, final int valLen) {
    if (changes == null) {
      assert buffer != null;
      assert buffer.order() == ByteOrder.nativeOrder();
      final byte[] result = new byte[valLen];

      buffer.position(pageOffset);
      buffer.get(result);

      return result;
    }

    return changes.getBinaryValue(buffer, pageOffset, valLen);
  }

  protected int getObjectSizeInDirectMemory(
      final OBinarySerializer<?> binarySerializer, final int offset) {
    if (changes == null) {
      assert buffer != null;
      assert buffer.order() == ByteOrder.nativeOrder();

      buffer.position(offset);
      return binarySerializer.getObjectSizeInByteBuffer(buffer);
    }

    return binarySerializer.getObjectSizeInByteBuffer(buffer, changes, offset);
  }

  protected <T> T deserializeFromDirectMemory(
      final OBinarySerializer<T> binarySerializer, final int offset) {
    if (changes == null) {
      assert buffer != null;
      assert buffer.order() == ByteOrder.nativeOrder();
      buffer.position(offset);
      return binarySerializer.deserializeFromByteBufferObject(buffer);
    }
    return binarySerializer.deserializeFromByteBufferObject(buffer, changes, offset);
  }

  protected final byte getByteValue(final int pageOffset) {
    if (changes == null) {

      assert buffer != null;

      assert buffer.order() == ByteOrder.nativeOrder();
      return buffer.get(pageOffset);
    }
    return changes.getByteValue(buffer, pageOffset);
  }

  @SuppressWarnings("SameReturnValue")
  protected final int setIntValue(final int pageOffset, final int value) {

    if (changes != null) {
      changes.setIntValue(buffer, value, pageOffset);
    } else {
      assert buffer != null;
      assert buffer.order() == ByteOrder.nativeOrder();
      buffer.putInt(pageOffset, value);
    }

    return OIntegerSerializer.INT_SIZE;
  }

  protected final int setShortValue(final int pageOffset, final short value) {

    if (changes != null) {
      changes.setIntValue(buffer, value, pageOffset);
    } else {
      assert buffer != null;
      assert buffer.order() == ByteOrder.nativeOrder();
      buffer.putShort(pageOffset, value);
    }

    return OShortSerializer.SHORT_SIZE;
  }

  @SuppressWarnings("SameReturnValue")
  protected final int setByteValue(final int pageOffset, final byte value) {
    if (changes != null) {
      changes.setByteValue(buffer, value, pageOffset);
    } else {
      assert buffer != null;
      assert buffer.order() == ByteOrder.nativeOrder();
      buffer.put(pageOffset, value);
    }

    return OByteSerializer.BYTE_SIZE;
  }

  @SuppressWarnings("SameReturnValue")
  protected final int setLongValue(final int pageOffset, final long value) {
    if (changes != null) {
      changes.setLongValue(buffer, value, pageOffset);
    } else {
      assert buffer != null;
      assert buffer.order() == ByteOrder.nativeOrder();
      buffer.putLong(pageOffset, value);
    }

    return OLongSerializer.LONG_SIZE;
  }

  protected final int setBinaryValue(final int pageOffset, final byte[] value) {
    if (value.length == 0) {
      return 0;
    }

    if (changes != null) {
      changes.setBinaryValue(buffer, value, pageOffset);
    } else {
      assert buffer != null;
      assert buffer.order() == ByteOrder.nativeOrder();
      buffer.position(pageOffset);
      buffer.put(value);
    }

    return value.length;
  }

  protected final void moveData(final int from, final int to, final int len) {
    if (len == 0) {
      return;
    }

    if (changes != null) {
      changes.moveData(buffer, from, to, len);
    } else {
      assert buffer != null;
      assert buffer.order() == ByteOrder.nativeOrder();
      final ByteBuffer rb = buffer.asReadOnlyBuffer();
      rb.position(from);
      rb.limit(from + len);

      buffer.position(to);
      buffer.put(rb);
    }
  }

  public OWALChanges getChanges() {
    return changes;
  }

  public final OCacheEntry getCacheEntry() {
    return cacheEntry;
  }

  public void restoreChanges(final OWALChanges changes) {
    final ByteBuffer buffer = cacheEntry.getCachePointer().getBuffer();
    assert buffer != null;

    buffer.position(0);
    changes.applyChanges(buffer);
  }

  public final void setLsn(final OLogSequenceNumber lsn) {
    assert buffer != null;

    assert buffer.order() == ByteOrder.nativeOrder();

    buffer.putLong(WAL_SEGMENT_OFFSET, lsn.getSegment());
    buffer.putInt(WAL_POSITION_OFFSET, lsn.getPosition());
  }

  public static void setPageLSN(final OLogSequenceNumber lsn, final OCacheEntry cacheEntry) {
    final ByteBuffer buffer = cacheEntry.getCachePointer().getBufferDuplicate();
    assert buffer != null;

    assert buffer.order() == ByteOrder.nativeOrder();

    buffer.putLong(WAL_SEGMENT_OFFSET, lsn.getSegment());
    buffer.putInt(WAL_POSITION_OFFSET, lsn.getPosition());
  }

  @Override
  public String toString() {
    if (cacheEntry != null) {
      return getClass().getSimpleName()
          + "{"
          + "fileId="
          + cacheEntry.getFileId()
          + ", pageIndex="
          + cacheEntry.getPageIndex()
          + '}';
    } else {
      return super.toString();
    }
  }
}

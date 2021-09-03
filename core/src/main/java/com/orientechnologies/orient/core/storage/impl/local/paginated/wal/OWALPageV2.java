/*
 * Copyright 2010-2013 OrientDB LTD (info--at--orientdb.com)
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
package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import java.nio.ByteBuffer;

/**
 * WAL page is organized using following format:
 *
 * <p>
 *
 * <ol>
 *   <li>CRC32 code of page content, it is used to check whether data are broken on disk. 4 bytes
 *   <li>Magic number, randomly generated number which is used to check whether page is broken on
 *       disk. 8 bytes
 *   <li>Amount of free space left on page, which can be used to add new records, 4 bytes
 *   <li>Position of LSN of last record which end is stored on this page, 8 bytes
 *   <li>End of the last record stored in page, 4 bytes
 *   <li>WAL records
 * </ol>
 *
 * <p>Each WAL record is stored using following format:
 *
 * <ol>
 *   <li>Flag which indicates that record should be merged with record which is stored on next page.
 *       That is needed if record can not be stored on one page and is split by two pages. 1 byte
 *   <li>Flag which indicates that this record is actually tail of long record parts of which are
 *       stored on other pages, 1 byte
 *   <li>Length of serialized content of WAL record. 4 bytes
 *   <li>Serialized content of the WAL record. Variable size.
 * </ol>
 *
 * <p>
 *
 * <p>Every time new record is added. Value of free space left on page is updated.
 *
 * @author Andrey Lomakin
 * @since 5/8/13
 */
public class OWALPageV2 implements OWALPage {
  /**
   * Value of magic number for v2 version of binary format
   *
   * @see OWALPage#MAGIC_NUMBER_OFFSET
   */
  static final long MAGIC_NUMBER = 0xEF30BCAFL;

  /**
   * Information about position of LSN of last record which end is stored on this page. If only
   * begging of the record is stored on this page its position is not stored.
   *
   * @see OLogSegmentV2.WriteTask#run()
   * @see OLogSegmentV2#init()
   */
  static final int LAST_STORED_LSN = FREE_SPACE_OFFSET + OIntegerSerializer.INT_SIZE;

  /**
   * End of the last record stored in page. This value is used in case of end of the WAL is broken
   * and we want to truncate it to read only valid values
   *
   * @see OLogSegmentV2.WriteTask#init()
   * @see OLogSegmentV2#init()
   */
  static final int END_LAST_RECORD = LAST_STORED_LSN + OLongSerializer.LONG_SIZE;

  /**
   * Offset inside of the page starting from which we will store new records till the end of the
   * page.
   */
  static final int RECORDS_OFFSET = END_LAST_RECORD + OIntegerSerializer.INT_SIZE;

  /**
   * Maximum size of the records which can be stored inside of the page
   *
   * @see #calculateRecordSize(int)
   * @see #calculateSerializedSize(int)
   */
  static final int MAX_ENTRY_SIZE = PAGE_SIZE - RECORDS_OFFSET;

  private final ByteBuffer buffer;

  OWALPageV2(ByteBuffer buffer, boolean isNew) {
    this.buffer = buffer;

    if (isNew) {
      buffer.position(MAGIC_NUMBER_OFFSET);

      buffer.putLong(MAGIC_NUMBER);
      buffer.putInt(MAX_ENTRY_SIZE);

      buffer.putLong(-1); // -1 means that we do not store any log record on this page
      buffer.putInt(-1); // -1 means that we do not store any log record on this page
    }
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getRecord(int position) {
    buffer.position(position + 2);
    final int recordSize = buffer.getInt();
    final byte[] record = new byte[recordSize];
    buffer.get(record);
    return record;
  }

  /** {@inheritDoc} */
  @Override
  public boolean mergeWithNextPage(int position) {
    return buffer.get(position) > 0;
  }

  /** {@inheritDoc} */
  @Override
  public int getFreeSpace() {
    return buffer.getInt(FREE_SPACE_OFFSET);
  }

  /**
   * Calculates how much space record will consume once it will be stored inside of page. Sizes are
   * different because once record is stored inside of the page, it is wrapped by additional system
   * information.
   */
  static int calculateSerializedSize(int recordSize) {
    return recordSize + OIntegerSerializer.INT_SIZE + 2;
  }

  /**
   * Calculates how much space record stored inside of page will consume once it will be read from
   * page. In other words it calculates initial size of the record before it was stored inside of
   * the page. Sizes are different because once record is stored inside of the page, it is wrapped
   * by additional system information.
   */
  static int calculateRecordSize(int serializedSize) {
    return serializedSize - OIntegerSerializer.INT_SIZE - 2;
  }
}

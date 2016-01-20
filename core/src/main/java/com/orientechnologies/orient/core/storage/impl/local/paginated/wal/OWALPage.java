/*
 * Copyright 2010-2013 Orient Technologies LTD (info--at--orientechnologies.com)
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
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

import java.nio.ByteBuffer;

/**
 * @author Andrey Lomakin
 * @since 5/8/13
 */
public class OWALPage {
  public static final long MAGIC_NUMBER    = 0xFACB03FEL;
  public static final int  PAGE_SIZE       = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;
  public static final int  MIN_RECORD_SIZE = OIntegerSerializer.INT_SIZE + 3;

  public static final  int CRC_OFFSET          = 0;
  public static final  int MAGIC_NUMBER_OFFSET = CRC_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int FREE_SPACE_OFFSET   = MAGIC_NUMBER_OFFSET + OLongSerializer.LONG_SIZE;
  public static final  int RECORDS_OFFSET      = FREE_SPACE_OFFSET + OIntegerSerializer.INT_SIZE;

  public static final int MAX_ENTRY_SIZE = PAGE_SIZE - RECORDS_OFFSET;

  private final ByteBuffer buffer;

  public OWALPage(ByteBuffer buffer, boolean isNew) {
    this.buffer = buffer;

    if (isNew) {
      buffer.position(MAGIC_NUMBER_OFFSET);

      buffer.putLong(MAGIC_NUMBER);
      buffer.putInt(MAX_ENTRY_SIZE);
    }
  }

  public ByteBuffer getByteBuffer() {
    return buffer;
  }

  public int appendRecord(byte[] content, boolean mergeWithNextPage, boolean recordTail) {
    int freeSpace = getFreeSpace();
    int freePosition = PAGE_SIZE - freeSpace;

    buffer.position(freePosition);
    buffer.put(mergeWithNextPage ? (byte) 1 : 0);

    buffer.put(recordTail ? (byte) 1 : 0);
    buffer.putInt(content.length);

    buffer.put(content);

    buffer.position(FREE_SPACE_OFFSET);
    buffer.putInt(FREE_SPACE_OFFSET, freeSpace - 2 - OIntegerSerializer.INT_SIZE - content.length);

    return freePosition;
  }

  public byte[] getRecord(int position) {
    buffer.position(position + 2);
    final int recordSize = buffer.getInt();
    final byte[] record = new byte[recordSize];
    buffer.get(record);
    return record;
  }

  public int getSerializedRecordSize(int position) {
    final int recordSize = buffer.getInt(position + 2);
    return recordSize + OIntegerSerializer.INT_SIZE + 2;
  }

  public boolean mergeWithNextPage(int position) {
    return buffer.get(position) > 0;
  }

  public boolean isEmpty() {
    return getFreeSpace() == MAX_ENTRY_SIZE;
  }

  public int getFreeSpace() {
    return buffer.getInt(FREE_SPACE_OFFSET);
  }

  public int getFilledUpTo() {
    return OWALPage.PAGE_SIZE - getFreeSpace();
  }

  public static int calculateSerializedSize(int recordSize) {
    return recordSize + OIntegerSerializer.INT_SIZE + 2;
  }

  public static int calculateRecordSize(int serializedSize) {
    return serializedSize - OIntegerSerializer.INT_SIZE - 2;
  }
}

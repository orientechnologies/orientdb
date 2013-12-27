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

package com.orientechnologies.orient.core.storage.impl.local.paginated.base;

import java.io.IOException;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageChanges;

/**
 * @author Andrey Lomakin
 * @since 16.08.13
 */
public class ODurablePage {
  protected static final int           MAGIC_NUMBER_OFFSET = 0;
  protected static final int           CRC32_OFFSET        = MAGIC_NUMBER_OFFSET + OLongSerializer.LONG_SIZE;

  public static final int              WAL_SEGMENT_OFFSET  = CRC32_OFFSET + OIntegerSerializer.INT_SIZE;
  public static final int              WAL_POSITION_OFFSET = WAL_SEGMENT_OFFSET + OLongSerializer.LONG_SIZE;
  public static final int              MAX_PAGE_SIZE_BYTES = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  protected static final int           NEXT_FREE_POSITION  = WAL_POSITION_OFFSET + OLongSerializer.LONG_SIZE;

  protected OPageChanges               pageChanges         = new OPageChanges();

  protected final ODirectMemoryPointer pagePointer;
  protected final TrackMode            trackMode;

  public ODurablePage(ODirectMemoryPointer pagePointer, TrackMode trackMode) {
    this.pagePointer = pagePointer;
    this.trackMode = trackMode;
  }

  public static OLogSequenceNumber getLogSequenceNumberFromPage(ODirectMemoryPointer dataPointer) {
    final long segment = OLongSerializer.INSTANCE.deserializeFromDirectMemory(dataPointer, WAL_SEGMENT_OFFSET);
    final long position = OLongSerializer.INSTANCE.deserializeFromDirectMemory(dataPointer, WAL_POSITION_OFFSET);

    return new OLogSequenceNumber(segment, position);
  }

  public static enum TrackMode {
    NONE, FULL, ROLLBACK_ONLY
  }

  protected int getIntValue(int pageOffset) {
    return OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(pagePointer, pageOffset);
  }

  protected long getLongValue(int pageOffset) {
    return OLongSerializer.INSTANCE.deserializeFromDirectMemory(pagePointer, pageOffset);
  }

  protected byte[] getBinaryValue(int pageOffset, int valLen) {
    return pagePointer.get(pageOffset, valLen);
  }

  protected byte getByteValue(int pageOffset) {
    return pagePointer.getByte(pageOffset);
  }

  protected int setIntValue(int pageOffset, int value) throws IOException {
    if (trackMode.equals(TrackMode.FULL)) {
      byte[] oldValues = pagePointer.get(pageOffset, OIntegerSerializer.INT_SIZE);
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(value, pagePointer, pageOffset);
      byte[] newValues = pagePointer.get(pageOffset, OIntegerSerializer.INT_SIZE);

      pageChanges.addChanges(pageOffset, newValues, oldValues);
    } else if (trackMode.equals(TrackMode.ROLLBACK_ONLY)) {
      byte[] oldValues = pagePointer.get(pageOffset, OIntegerSerializer.INT_SIZE);
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(value, pagePointer, pageOffset);

      pageChanges.addChanges(pageOffset, null, oldValues);
    } else
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(value, pagePointer, pageOffset);
    return OIntegerSerializer.INT_SIZE;

  }

  protected int setByteValue(int pageOffset, byte value) {
    if (trackMode.equals(TrackMode.FULL)) {
      byte[] oldValues = new byte[] { pagePointer.getByte(pageOffset) };
      pagePointer.setByte(pageOffset, value);
      byte[] newValues = new byte[] { pagePointer.getByte(pageOffset) };

      pageChanges.addChanges(pageOffset, newValues, oldValues);
    } else if (trackMode.equals(TrackMode.ROLLBACK_ONLY)) {
      byte[] oldValues = new byte[] { pagePointer.getByte(pageOffset) };
      pagePointer.setByte(pageOffset, value);

      pageChanges.addChanges(pageOffset, null, oldValues);
    } else
      pagePointer.setByte(pageOffset, value);
    return OByteSerializer.BYTE_SIZE;
  }

  protected int setLongValue(int pageOffset, long value) throws IOException {
    if (trackMode.equals(TrackMode.FULL)) {
      byte[] oldValues = pagePointer.get(pageOffset, OLongSerializer.LONG_SIZE);
      OLongSerializer.INSTANCE.serializeInDirectMemory(value, pagePointer, pageOffset);
      byte[] newValues = pagePointer.get(pageOffset, OLongSerializer.LONG_SIZE);

      pageChanges.addChanges(pageOffset, newValues, oldValues);
    } else if (trackMode.equals(TrackMode.ROLLBACK_ONLY)) {
      byte[] oldValues = pagePointer.get(pageOffset, OLongSerializer.LONG_SIZE);
      OLongSerializer.INSTANCE.serializeInDirectMemory(value, pagePointer, pageOffset);

      pageChanges.addChanges(pageOffset, null, oldValues);
    } else
      OLongSerializer.INSTANCE.serializeInDirectMemory(value, pagePointer, pageOffset);
    return OLongSerializer.LONG_SIZE;
  }

  protected int setBinaryValue(int pageOffset, byte[] value) throws IOException {
    if (value.length == 0)
      return 0;

    if (trackMode.equals(TrackMode.FULL)) {
      byte[] oldValues = pagePointer.get(pageOffset, value.length);
      pagePointer.set(pageOffset, value, 0, value.length);

      pageChanges.addChanges(pageOffset, value, oldValues);
    } else if (trackMode.equals(TrackMode.ROLLBACK_ONLY)) {
      byte[] oldValues = pagePointer.get(pageOffset, value.length);
      pagePointer.set(pageOffset, value, 0, value.length);

      pageChanges.addChanges(pageOffset, null, oldValues);
    } else
      pagePointer.set(pageOffset, value, 0, value.length);
    return value.length;
  }

  protected void moveData(int from, int to, int len) throws IOException {
    if (len == 0)
      return;

    if (trackMode.equals(TrackMode.FULL)) {
      byte[] content = pagePointer.get(from, len);
      byte[] oldContent = pagePointer.get(to, len);

      pagePointer.moveData(from, pagePointer, to, len);

      pageChanges.addChanges(to, content, oldContent);
    } else if (trackMode.equals(TrackMode.ROLLBACK_ONLY)) {
      byte[] oldContent = pagePointer.get(to, len);

      pagePointer.moveData(from, pagePointer, to, len);

      pageChanges.addChanges(to, null, oldContent);

    } else
      pagePointer.moveData(from, pagePointer, to, len);
  }

  public OPageChanges getPageChanges() {
    return pageChanges;
  }

  public void restoreChanges(OPageChanges pageChanges) {
    pageChanges.applyChanges(pagePointer);
  }

  public void revertChanges(OPageChanges pageChanges) {
    pageChanges.revertChanges(pagePointer);
  }

  public OLogSequenceNumber getLsn() {
    final long segment = getLongValue(WAL_SEGMENT_OFFSET);
    final long position = getLongValue(WAL_POSITION_OFFSET);

    return new OLogSequenceNumber(segment, position);
  }

  public void setLsn(OLogSequenceNumber lsn) {
    OLongSerializer.INSTANCE.serializeInDirectMemory(lsn.getSegment(), pagePointer, WAL_SEGMENT_OFFSET);
    OLongSerializer.INSTANCE.serializeInDirectMemory(lsn.getPosition(), pagePointer, WAL_POSITION_OFFSET);
  }
}

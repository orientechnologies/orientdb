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

package com.orientechnologies.common.directmemory;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OCharSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

/**
 * Abstraction of pointer which points to allocated direct memory area. All access to direct memory should be performed ONLY using
 * this class instance.
 *
 * Instance of this class cannot be created directly. If you need to work with direct memory use following approach.
 *
 * <code>
 *   ODirectMemory directMemory = ODirectMemoryFactory.directMemory();
 *   ODirectMemoryPointer pointer = directMemory.allocate(1024); //size in bytes
 *   //..do something
 *   pointer.free();
 * </code>
 *
 * but usually you will work with disk based data structures which work using disk cache so you will not allocate direct memory by
 * yourself.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 10/19/13
 */
public class ODirectMemoryPointer {
  private final boolean       SAFE_MODE    = OGlobalConfiguration.DIRECT_MEMORY_SAFE_MODE.getValueAsBoolean();

  private final ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

  private final long          pageSize;
  private final long          dataPointer;

  public ODirectMemoryPointer(final long pageSize) {
    if (pageSize <= 0)
      throw new ODirectMemoryViolationException("Size of allocated area should be more than zero but " + pageSize
          + " was provided.");

    this.dataPointer = directMemory.allocate(pageSize);
    this.pageSize = pageSize;
  }

  public ODirectMemoryPointer(final byte[] data) {
    if (data.length == 0)
      throw new ODirectMemoryViolationException("Size of allocated area should be more than zero but 0 was provided.");
    this.pageSize = data.length;
    this.dataPointer = directMemory.allocate(pageSize);

    set(0, data, 0, data.length);
  }

  public byte[] get(final long offset, final int length) {
    if (SAFE_MODE)
      rangeCheck(offset, length);

    return directMemory.get(dataPointer + offset, length);
  }

  public void get(final long offset, final byte[] array, final int arrayOffset, final int length) {
    if (SAFE_MODE)
      rangeCheck(offset, length);

    directMemory.get(dataPointer + offset, array, arrayOffset, length);
  }

  public void set(final long offset, final byte[] content, final int arrayOffset, final int length) {
    if (SAFE_MODE)
      rangeCheck(offset, length);

    directMemory.set(dataPointer + offset, content, arrayOffset, length);
  }

  public int getInt(final long offset) {
    if (SAFE_MODE)
      rangeCheck(offset, OIntegerSerializer.INT_SIZE);

    return directMemory.getInt(dataPointer + offset);
  }

  public void setInt(final long offset, final int value) {
    if (SAFE_MODE)
      rangeCheck(offset, OIntegerSerializer.INT_SIZE);

    directMemory.setInt(dataPointer + offset, value);
  }

  public void setShort(final long offset, final short value) {
    if (SAFE_MODE)
      rangeCheck(offset, OShortSerializer.SHORT_SIZE);

    directMemory.setShort(dataPointer + offset, value);
  }

  public short getShort(final long offset) {
    if (SAFE_MODE)
      rangeCheck(offset, OShortSerializer.SHORT_SIZE);

    return directMemory.getShort(dataPointer + offset);
  }

  public long getLong(final long offset) {
    if (SAFE_MODE)
      rangeCheck(offset, OLongSerializer.LONG_SIZE);

    return directMemory.getLong(dataPointer + offset);
  }

  public void setLong(final long offset, final long value) {
    if (SAFE_MODE)
      rangeCheck(offset, OLongSerializer.LONG_SIZE);

    directMemory.setLong(dataPointer + offset, value);
  }

  public byte getByte(final long offset) {
    if (SAFE_MODE)
      rangeCheck(offset, OByteSerializer.BYTE_SIZE);

    return directMemory.getByte(dataPointer + offset);
  }

  public void setByte(final long offset, final byte value) {
    if (SAFE_MODE)
      rangeCheck(offset, OByteSerializer.BYTE_SIZE);

    directMemory.setByte(dataPointer + offset, value);
  }

  public void setChar(final long offset, final char value) {
    if (SAFE_MODE)
      rangeCheck(offset, OCharSerializer.CHAR_SIZE);

    directMemory.setChar(dataPointer + offset, value);
  }

  public char getChar(final long offset) {
    if (SAFE_MODE)
      rangeCheck(offset, OCharSerializer.CHAR_SIZE);

    return directMemory.getChar(dataPointer + offset);
  }

  public void moveData(final long srcOffset, ODirectMemoryPointer destPointer, final long destOffset, final long len) {
    if (SAFE_MODE) {
      rangeCheck(srcOffset, len);
      rangeCheck(destOffset, len);
    }

    directMemory.moveData(dataPointer + srcOffset, destPointer.getDataPointer() + destOffset, len);
  }

  public long getDataPointer() {
    return dataPointer;
  }

  public void free() {
    directMemory.free(dataPointer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    ODirectMemoryPointer that = (ODirectMemoryPointer) o;

    if (dataPointer != that.dataPointer)
      return false;
    if (pageSize != that.pageSize)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (pageSize ^ (pageSize >>> 32));
    result = 31 * result + (int) (dataPointer ^ (dataPointer >>> 32));
    return result;
  }

  private void rangeCheck(final long offset, final long size) {
    if (offset < 0)
      throw new ODirectMemoryViolationException("Negative offset was provided");

    if (size < 0)
      throw new ODirectMemoryViolationException("Negative size was provided");

    if (offset > pageSize)
      throw new ODirectMemoryViolationException("Provided offset [" + offset + "] is more than size of allocated area  ["
          + pageSize + "]");

    if (offset + size > pageSize)
      throw new ODirectMemoryViolationException("Last position of provided data interval [" + (offset + size)
          + "] is more than size of allocated area [" + pageSize + "]");
  }
}

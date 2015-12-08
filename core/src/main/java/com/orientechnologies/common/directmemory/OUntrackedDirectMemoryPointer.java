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
 * <p>
 * Instance of this class cannot be created directly. If you need to work with direct memory use following approach.
 * <p>
 * <code>
 * ODirectMemory directMemory = ODirectMemoryFactory.directMemory();
 * ODirectMemoryPointer pointer = directMemory.allocate(1024); //size in bytes
 * //..do something
 * pointer.free();
 * </code>
 * <p>
 * but usually you will work with disk based data structures which work using disk cache so you will not allocate direct memory by
 * yourself.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 10/19/13
 */
public class OUntrackedDirectMemoryPointer implements ODirectMemoryPointer {
  private final boolean       safeMode;
  private final ODirectMemory directMemory;

  private final long pageSize;
  private final long dataPointer;

  OUntrackedDirectMemoryPointer(boolean safeMode, ODirectMemory directMemory, final long pageSize,
      ODirectMemoryPointerFactory factory) {
    this.safeMode = safeMode;
    this.directMemory = directMemory;
    if (pageSize <= 0)
      throw new ODirectMemoryViolationException(
          "Size of allocated area should be more than zero but " + pageSize + " was provided.");

    this.dataPointer = this.directMemory.allocate(pageSize);
    this.pageSize = pageSize;
  }

  OUntrackedDirectMemoryPointer(final byte[] data, boolean safeMode, ODirectMemory directMemory,
      ODirectMemoryPointerFactory factory) {
    this.safeMode = safeMode;
    this.directMemory = directMemory;
    if (data.length == 0)
      throw new ODirectMemoryViolationException("Size of allocated area should be more than zero but 0 was provided.");
    this.pageSize = data.length;
    this.dataPointer = this.directMemory.allocate(pageSize);

    set(0, data, 0, data.length);
  }

  @Override
  public byte[] get(final long offset, final int length) {
    if (safeMode)
      rangeCheck(offset, length);

    return directMemory.get(dataPointer + offset, length);
  }

  @Override
  public void get(final long offset, final byte[] array, final int arrayOffset, final int length) {
    if (safeMode)
      rangeCheck(offset, length);

    directMemory.get(dataPointer + offset, array, arrayOffset, length);
  }

  @Override
  public void set(final long offset, final byte[] content, final int arrayOffset, final int length) {
    if (safeMode)
      rangeCheck(offset, length);

    directMemory.set(dataPointer + offset, content, arrayOffset, length);
  }

  @Override
  public int getInt(final long offset) {
    if (safeMode)
      rangeCheck(offset, OIntegerSerializer.INT_SIZE);

    return directMemory.getInt(dataPointer + offset);
  }

  @Override
  public void setInt(final long offset, final int value) {
    if (safeMode)
      rangeCheck(offset, OIntegerSerializer.INT_SIZE);

    directMemory.setInt(dataPointer + offset, value);
  }

  @Override
  public void setShort(final long offset, final short value) {
    if (safeMode)
      rangeCheck(offset, OShortSerializer.SHORT_SIZE);

    directMemory.setShort(dataPointer + offset, value);
  }

  @Override
  public short getShort(final long offset) {
    if (safeMode)
      rangeCheck(offset, OShortSerializer.SHORT_SIZE);

    return directMemory.getShort(dataPointer + offset);
  }

  @Override
  public long getLong(final long offset) {
    if (safeMode)
      rangeCheck(offset, OLongSerializer.LONG_SIZE);

    return directMemory.getLong(dataPointer + offset);
  }

  @Override
  public void setLong(final long offset, final long value) {
    if (safeMode)
      rangeCheck(offset, OLongSerializer.LONG_SIZE);

    directMemory.setLong(dataPointer + offset, value);
  }

  @Override
  public byte getByte(final long offset) {
    if (safeMode)
      rangeCheck(offset, OByteSerializer.BYTE_SIZE);

    return directMemory.getByte(dataPointer + offset);
  }

  @Override
  public void setByte(final long offset, final byte value) {
    if (safeMode)
      rangeCheck(offset, OByteSerializer.BYTE_SIZE);

    directMemory.setByte(dataPointer + offset, value);
  }

  @Override
  public void setChar(final long offset, final char value) {
    if (safeMode)
      rangeCheck(offset, OCharSerializer.CHAR_SIZE);

    directMemory.setChar(dataPointer + offset, value);
  }

  @Override
  public char getChar(final long offset) {
    if (safeMode)
      rangeCheck(offset, OCharSerializer.CHAR_SIZE);

    return directMemory.getChar(dataPointer + offset);
  }

  @Override
  public void moveData(final long srcOffset, ODirectMemoryPointer destPointer, final long destOffset, final long len) {
    if (safeMode) {
      rangeCheck(srcOffset, len);
      rangeCheck(destOffset, len);
    }

    directMemory.moveData(dataPointer + srcOffset, destPointer.getDataPointer() + destOffset, len);
  }

  @Override
  public long getDataPointer() {
    return dataPointer;
  }

  @Override
  public void free() {
    directMemory.free(dataPointer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OUntrackedDirectMemoryPointer that = (OUntrackedDirectMemoryPointer) o;

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
      throw new ODirectMemoryViolationException(
          "Provided offset [" + offset + "] is more than size of allocated area  [" + pageSize + "]");

    if (offset + size > pageSize)
      throw new ODirectMemoryViolationException(
          "Last position of provided data interval [" + (offset + size) + "] is more than size of allocated area [" + pageSize
              + "]");
  }
}

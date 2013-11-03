package com.orientechnologies.common.directmemory;

import com.orientechnologies.common.serialization.types.*;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 10/19/13
 */
public class ODirectMemoryPointer {
  private final ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

  private final long          pageSize;
  private final long          dataPointer;

  public ODirectMemoryPointer(long pageSize) {
    if (pageSize <= 0)
      throw new ODirectMemoryViolationException("Size of allocated area should be more than zero but " + pageSize
          + " was provided.");

    this.dataPointer = directMemory.allocate(pageSize);
    this.pageSize = pageSize;
  }

  public ODirectMemoryPointer(byte[] data) {
    if (data.length == 0)
      throw new ODirectMemoryViolationException("Size of allocated area should be more than zero but 0 was provided.");

    this.dataPointer = directMemory.allocate(data);
    this.pageSize = data.length;
  }

  public byte[] get(long offset, int length) {
    rangeCheck(offset, length);

    return directMemory.get(dataPointer + offset, length);
  }

  public void get(long offset, byte[] array, int arrayOffset, int length) {
    rangeCheck(offset, length);

    directMemory.get(dataPointer + offset, array, arrayOffset, length);
  }

  public void set(long offset, byte[] content, int arrayOffset, int length) {
    rangeCheck(offset, length);

    directMemory.set(dataPointer + offset, content, arrayOffset, length);
  }

  public int getInt(long offset) {
    rangeCheck(offset, OIntegerSerializer.INT_SIZE);

    return directMemory.getInt(dataPointer + offset);
  }

  public void setInt(long offset, int value) {
    rangeCheck(offset, OIntegerSerializer.INT_SIZE);

    directMemory.setInt(dataPointer + offset, value);
  }

  public void setShort(long offset, short value) {
    rangeCheck(offset, OShortSerializer.SHORT_SIZE);

    directMemory.setShort(dataPointer + offset, value);
  }

  public short getShort(long offset) {
    rangeCheck(offset, OShortSerializer.SHORT_SIZE);

    return directMemory.getShort(dataPointer + offset);
  }

  public long getLong(long offset) {
    rangeCheck(offset, OLongSerializer.LONG_SIZE);

    return directMemory.getLong(dataPointer + offset);
  }

  public void setLong(long offset, long value) {
    rangeCheck(offset, OLongSerializer.LONG_SIZE);

    directMemory.setLong(dataPointer + offset, value);
  }

  public byte getByte(long offset) {
    rangeCheck(offset, OByteSerializer.BYTE_SIZE);

    return directMemory.getByte(dataPointer + offset);
  }

  public void setByte(long offset, byte value) {
    rangeCheck(offset, OByteSerializer.BYTE_SIZE);

    directMemory.setByte(dataPointer + offset, value);
  }

  public void setChar(long offset, char value) {
    rangeCheck(offset, OCharSerializer.CHAR_SIZE);

    directMemory.setChar(dataPointer + offset, value);
  }

  public char getChar(long offset) {
    rangeCheck(offset, OCharSerializer.CHAR_SIZE);

    return directMemory.getChar(dataPointer + offset);
  }

  public void copyData(long srcOffset, ODirectMemoryPointer destPointer, long destOffset, long len) {
    rangeCheck(srcOffset, len);
    rangeCheck(destOffset, len);

    directMemory.copyData(dataPointer + srcOffset, destPointer.getDataPointer() + destOffset, len);
  }

  private void rangeCheck(long offset, long size) {
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
}

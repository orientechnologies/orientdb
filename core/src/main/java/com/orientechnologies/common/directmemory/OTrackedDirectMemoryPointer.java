package com.orientechnologies.common.directmemory;

import com.orientechnologies.common.serialization.types.*;

import java.util.Arrays;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 9/8/2015
 */
public class OTrackedDirectMemoryPointer implements ODirectMemoryPointer {
  private final    long                        pageSize;
  private volatile long                        dataPointer;
  private volatile StackTraceElement[]         allocationStackTraceElements;
  private final    ODirectMemoryPointerFactory factory;

  private final boolean       safeMode;
  private final ODirectMemory directMemory;

  OTrackedDirectMemoryPointer(final long pageSize, ODirectMemoryPointerFactory factory, final boolean safeMode,
      final ODirectMemory directMemory) {
    this.factory = factory;
    if (pageSize <= 0)
      throw new ODirectMemoryViolationException(
          "Size of allocated area should be more than zero but " + pageSize + " was provided.");

    this.safeMode = safeMode;
    this.directMemory = directMemory;
    this.dataPointer = directMemory.allocate(pageSize);
    this.pageSize = pageSize;

    final StackTraceElement[] ste = Thread.currentThread().getStackTrace();
    allocationStackTraceElements = Arrays.copyOfRange(ste, 1, ste.length);
  }

  OTrackedDirectMemoryPointer(final byte[] data, ODirectMemoryPointerFactory factory, final boolean safeMode,
      final ODirectMemory directMemory) {
    this.factory = factory;
    if (data.length == 0)
      throw new ODirectMemoryViolationException("Size of allocated area should be more than zero but 0 was provided.");

    this.safeMode = safeMode;
    this.directMemory = directMemory;
    this.pageSize = data.length;
    this.dataPointer = directMemory.allocate(pageSize);

    final StackTraceElement[] ste = Thread.currentThread().getStackTrace();
    allocationStackTraceElements = Arrays.copyOf(ste, ste.length - 1);

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
    factory.memoryFreed(pageSize);

    dataPointer = 0;
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();

    if (dataPointer > 0) {
      factory.memoryLeakDetected(allocationStackTraceElements);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OTrackedDirectMemoryPointer that = (OTrackedDirectMemoryPointer) o;

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

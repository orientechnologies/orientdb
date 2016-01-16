package com.orientechnologies.common.directmemory;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 9/8/2015
 */
public interface ODirectMemoryPointer {
  byte[] get(long offset, int length);

  void get(long offset, byte[] array, int arrayOffset, int length);

  void set(long offset, byte[] content, int arrayOffset, int length);

  int getInt(long offset);

  void setInt(long offset, int value);

  void setShort(long offset, short value);

  short getShort(long offset);

  long getLong(long offset);

  void setLong(long offset, long value);

  byte getByte(long offset);

  void setByte(long offset, byte value);

  void setChar(long offset, char value);

  char getChar(long offset);

  void moveData(long srcOffset, ODirectMemoryPointer destPointer, long destOffset, long len);

  long getDataPointer();

  void free();
}

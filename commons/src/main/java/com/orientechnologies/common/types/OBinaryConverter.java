package com.orientechnologies.common.types;

/**
 * @author Andrey Lomakin
 * @since 26.07.12
 */
public interface OBinaryConverter {
  void putInt(byte[] buffer, int index, int value);

  int getInt(byte[] buffer, int index);

  void putShort(byte[] buffer, int index, short value);

  short getShort(byte[] buffer, int index);

  void putLong(byte[] buffer, int index, long value);

  long getLong(byte[] buffer, int index);

  void putChar(byte[] buffer, int index, char character);

  char getChar(byte[] buffer, int index);

  boolean nativeAccelerationUsed();
}

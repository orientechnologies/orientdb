package com.orientechnologies.common.serialization;

import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;

import sun.misc.Unsafe;

/**
 * @author Andrey Lomakin
 * @since 26.07.12
 */
@SuppressWarnings("restriction")
public class OUnsafeBinaryConverter implements OBinaryConverter {
  public static final OUnsafeBinaryConverter INSTANCE = new OUnsafeBinaryConverter();

  private static final Unsafe                theUnsafe;
  private static final long                  BYTE_ARRAY_OFFSET;

  static {
    theUnsafe = (Unsafe) AccessController.doPrivileged(new PrivilegedAction<Object>() {
      public Object run() {
        try {
          Field f = Unsafe.class.getDeclaredField("theUnsafe");
          boolean wasAccessible = f.isAccessible();
          f.setAccessible(true);
          try {
            return f.get(null);
          } finally {
            f.setAccessible(wasAccessible);
          }

        } catch (NoSuchFieldException e) {
          throw new Error();
        } catch (IllegalAccessException e) {
          throw new Error();
        }
      }
    });
    BYTE_ARRAY_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);
  }

  public void putShort(byte[] buffer, int index, short value, ByteOrder byteOrder) {
    if (!byteOrder.equals(ByteOrder.nativeOrder()))
      value = Short.reverseBytes(value);

    theUnsafe.putShort(buffer, index + BYTE_ARRAY_OFFSET, value);
  }

  public short getShort(byte[] buffer, int index, ByteOrder byteOrder) {
    short result = theUnsafe.getShort(buffer, index + BYTE_ARRAY_OFFSET);
    if (!byteOrder.equals(ByteOrder.nativeOrder()))
      result = Short.reverseBytes(result);

    return result;
  }

  public void putInt(byte[] buffer, int pointer, int value, ByteOrder byteOrder) {
    final long position = pointer + BYTE_ARRAY_OFFSET;
    if (!byteOrder.equals(ByteOrder.nativeOrder()))
      value = Integer.reverseBytes(value);

    theUnsafe.putInt(buffer, position, value);
  }

  public int getInt(byte[] buffer, int pointer, ByteOrder byteOrder) {
    final long position = pointer + BYTE_ARRAY_OFFSET;
    int result = theUnsafe.getInt(buffer, position);
    if (!byteOrder.equals(ByteOrder.nativeOrder()))
      result = Integer.reverseBytes(result);

    return result;
  }

  public void putLong(byte[] buffer, int index, long value, ByteOrder byteOrder) {
    if (!byteOrder.equals(ByteOrder.nativeOrder()))
      value = Long.reverseBytes(value);

    theUnsafe.putLong(buffer, index + BYTE_ARRAY_OFFSET, value);
  }

  public long getLong(byte[] buffer, int index, ByteOrder byteOrder) {
    long result = theUnsafe.getLong(buffer, index + BYTE_ARRAY_OFFSET);
    if (!byteOrder.equals(ByteOrder.nativeOrder()))
      result = Long.reverseBytes(result);

    return result;
  }

  public void putChar(byte[] buffer, int index, char character, ByteOrder byteOrder) {
    if (!byteOrder.equals(ByteOrder.nativeOrder()))
      character = Character.reverseBytes(character);

    theUnsafe.putChar(buffer, index + BYTE_ARRAY_OFFSET, character);
  }

  public char getChar(byte[] buffer, int index, ByteOrder byteOrder) {
    char result = theUnsafe.getChar(buffer, index + BYTE_ARRAY_OFFSET);
    if (!byteOrder.equals(ByteOrder.nativeOrder()))
      result = Character.reverseBytes(result);

    return result;
  }

  public boolean nativeAccelerationUsed() {
    return true;
  }
}

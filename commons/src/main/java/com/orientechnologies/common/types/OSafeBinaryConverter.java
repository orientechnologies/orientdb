package com.orientechnologies.common.types;

public class OSafeBinaryConverter implements OBinaryConverter {
  public void putShort(byte[] buffer, int index, short value) {
    short2bytes(value, buffer, index);
  }

  public short getShort(byte[] buffer, int index) {
    return bytes2short(buffer, index);
  }

  public void putInt(byte[] buffer, int pointer, int offset, int value) {
    final int position = pointer + offset;

    int2bytes(value, buffer, position);
  }

  public int getInt(byte[] buffer, int pointer, int offset) {
    final int position = pointer + offset;

    return bytes2int(buffer, position);
  }

  public void putLong(byte[] buffer, int index, long value) {
    long2bytes(value, buffer, index);
  }

  public long getLong(byte[] buffer, int index) {
    return bytes2long(buffer, index);
  }

  public void putChar(byte[] buffer, int index, char character) {
    buffer[index] = (byte) (character >>> 8);
    buffer[index + 1] = (byte) character;
  }

  public char getChar(byte[] buffer, int index) {
    return (char) (((buffer[index] & 0xFF) << 8) + (buffer[index + 1] & 0xFF));
  }

  public boolean nativeAccelerationUsed() {
    return false;
  }

  private static byte[] short2bytes(final short value, final byte[] b, final int iBeginOffset) {
    b[iBeginOffset] = (byte) ((value >>> 8) & 0xFF);
    b[iBeginOffset + 1] = (byte) (value & 0xFF);
    return b;
  }

  private static short bytes2short(final byte[] b, final int offset) {
    return (short) ((b[offset] << 8) | (b[offset + 1] & 0xff));
  }

  private static int bytes2int(final byte[] b, final int offset) {
    return (b[offset]) << 24 | (0xff & b[offset + 1]) << 16 | (0xff & b[offset + 2]) << 8 | ((0xff & b[offset + 3]));
  }

  private static byte[] int2bytes(final int value, final byte[] b, final int iBeginOffset) {
    b[iBeginOffset] = (byte) ((value >>> 24) & 0xFF);
    b[iBeginOffset + 1] = (byte) ((value >>> 16) & 0xFF);
    b[iBeginOffset + 2] = (byte) ((value >>> 8) & 0xFF);
    b[iBeginOffset + 3] = (byte) (value & 0xFF);
    return b;
  }

  private static byte[] long2bytes(final long value, final byte[] b, final int iBeginOffset) {
    b[iBeginOffset] = (byte) ((value >>> 56) & 0xFF);
    b[iBeginOffset + 1] = (byte) ((value >>> 48) & 0xFF);
    b[iBeginOffset + 2] = (byte) ((value >>> 40) & 0xFF);
    b[iBeginOffset + 3] = (byte) ((value >>> 32) & 0xFF);
    b[iBeginOffset + 4] = (byte) ((value >>> 24) & 0xFF);
    b[iBeginOffset + 5] = (byte) ((value >>> 16) & 0xFF);
    b[iBeginOffset + 6] = (byte) ((value >>> 8) & 0xFF);
    b[iBeginOffset + 7] = (byte) (value & 0xFF);
    return b;
  }

  private static long bytes2long(final byte[] b, final int offset) {
    return ((0xff & b[offset + 7]) | (0xff & b[offset + 6]) << 8 | (0xff & b[offset + 5]) << 16
        | (long) (0xff & b[offset + 4]) << 24 | (long) (0xff & b[offset + 3]) << 32 | (long) (0xff & b[offset + 2]) << 40
        | (long) (0xff & b[offset + 1]) << 48 | (long) (0xff & b[offset]) << 56);
  }
}

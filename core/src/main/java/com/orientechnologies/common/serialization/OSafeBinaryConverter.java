/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.common.serialization;

import java.nio.ByteOrder;

public class OSafeBinaryConverter implements OBinaryConverter {
  public static final OSafeBinaryConverter INSTANCE = new OSafeBinaryConverter();

  public void putShort(byte[] buffer, int index, short value, ByteOrder byteOrder) {
    if (byteOrder.equals(ByteOrder.BIG_ENDIAN)) short2BytesBigEndian(value, buffer, index);
    else short2BytesLittleEndian(value, buffer, index);
  }

  public short getShort(byte[] buffer, int index, ByteOrder byteOrder) {
    if (byteOrder.equals(ByteOrder.BIG_ENDIAN)) return bytes2ShortBigEndian(buffer, index);

    return bytes2ShortLittleEndian(buffer, index);
  }

  public void putInt(byte[] buffer, int pointer, int value, ByteOrder byteOrder) {
    if (byteOrder.equals(ByteOrder.BIG_ENDIAN)) int2BytesBigEndian(value, buffer, pointer);
    else int2BytesLittleEndian(value, buffer, pointer);
  }

  public int getInt(byte[] buffer, int pointer, ByteOrder byteOrder) {
    if (byteOrder.equals(ByteOrder.BIG_ENDIAN)) return bytes2IntBigEndian(buffer, pointer);

    return bytes2IntLittleEndian(buffer, pointer);
  }

  public void putLong(byte[] buffer, int index, long value, ByteOrder byteOrder) {
    if (byteOrder.equals(ByteOrder.BIG_ENDIAN)) long2BytesBigEndian(value, buffer, index);
    else long2BytesLittleEndian(value, buffer, index);
  }

  public long getLong(byte[] buffer, int index, ByteOrder byteOrder) {
    if (byteOrder.equals(ByteOrder.BIG_ENDIAN)) return bytes2LongBigEndian(buffer, index);

    return bytes2LongLittleEndian(buffer, index);
  }

  public void putChar(byte[] buffer, int index, char character, ByteOrder byteOrder) {
    if (byteOrder.equals(ByteOrder.BIG_ENDIAN)) {
      buffer[index] = (byte) (character >>> 8);
      buffer[index + 1] = (byte) character;
    } else {
      buffer[index + 1] = (byte) (character >>> 8);
      buffer[index] = (byte) character;
    }
  }

  public char getChar(byte[] buffer, int index, ByteOrder byteOrder) {
    if (byteOrder.equals(ByteOrder.BIG_ENDIAN))
      return (char) (((buffer[index] & 0xFF) << 8) + (buffer[index + 1] & 0xFF));

    return (char) (((buffer[index + 1] & 0xFF) << 8) + (buffer[index] & 0xFF));
  }

  public boolean nativeAccelerationUsed() {
    return false;
  }

  private static byte[] short2BytesBigEndian(
      final short value, final byte[] b, final int iBeginOffset) {
    b[iBeginOffset] = (byte) ((value >>> 8) & 0xFF);
    b[iBeginOffset + 1] = (byte) (value & 0xFF);
    return b;
  }

  private static byte[] short2BytesLittleEndian(
      final short value, final byte[] b, final int iBeginOffset) {
    b[iBeginOffset + 1] = (byte) ((value >>> 8) & 0xFF);
    b[iBeginOffset] = (byte) (value & 0xFF);
    return b;
  }

  private static short bytes2ShortBigEndian(final byte[] b, final int offset) {
    return (short) ((b[offset] << 8) | (b[offset + 1] & 0xff));
  }

  private static short bytes2ShortLittleEndian(final byte[] b, final int offset) {
    return (short) ((b[offset + 1] << 8) | (b[offset] & 0xff));
  }

  private static int bytes2IntBigEndian(final byte[] b, final int offset) {
    return (b[offset]) << 24
        | (0xff & b[offset + 1]) << 16
        | (0xff & b[offset + 2]) << 8
        | ((0xff & b[offset + 3]));
  }

  private static int bytes2IntLittleEndian(final byte[] b, final int offset) {
    return (b[offset + 3]) << 24
        | (0xff & b[offset + 2]) << 16
        | (0xff & b[offset + 1]) << 8
        | ((0xff & b[offset]));
  }

  private static byte[] int2BytesBigEndian(
      final int value, final byte[] b, final int iBeginOffset) {
    b[iBeginOffset] = (byte) ((value >>> 24) & 0xFF);
    b[iBeginOffset + 1] = (byte) ((value >>> 16) & 0xFF);
    b[iBeginOffset + 2] = (byte) ((value >>> 8) & 0xFF);
    b[iBeginOffset + 3] = (byte) (value & 0xFF);
    return b;
  }

  private static byte[] int2BytesLittleEndian(
      final int value, final byte[] b, final int iBeginOffset) {
    b[iBeginOffset + 3] = (byte) ((value >>> 24) & 0xFF);
    b[iBeginOffset + 2] = (byte) ((value >>> 16) & 0xFF);
    b[iBeginOffset + 1] = (byte) ((value >>> 8) & 0xFF);
    b[iBeginOffset] = (byte) (value & 0xFF);
    return b;
  }

  private static byte[] long2BytesBigEndian(
      final long value, final byte[] b, final int iBeginOffset) {
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

  private static byte[] long2BytesLittleEndian(
      final long value, final byte[] b, final int iBeginOffset) {
    b[iBeginOffset + 7] = (byte) ((value >>> 56) & 0xFF);
    b[iBeginOffset + 6] = (byte) ((value >>> 48) & 0xFF);
    b[iBeginOffset + 5] = (byte) ((value >>> 40) & 0xFF);
    b[iBeginOffset + 4] = (byte) ((value >>> 32) & 0xFF);
    b[iBeginOffset + 3] = (byte) ((value >>> 24) & 0xFF);
    b[iBeginOffset + 2] = (byte) ((value >>> 16) & 0xFF);
    b[iBeginOffset + 1] = (byte) ((value >>> 8) & 0xFF);
    b[iBeginOffset] = (byte) (value & 0xFF);
    return b;
  }

  private static long bytes2LongBigEndian(final byte[] b, final int offset) {
    return ((0xff & b[offset + 7])
        | (0xff & b[offset + 6]) << 8
        | (0xff & b[offset + 5]) << 16
        | (long) (0xff & b[offset + 4]) << 24
        | (long) (0xff & b[offset + 3]) << 32
        | (long) (0xff & b[offset + 2]) << 40
        | (long) (0xff & b[offset + 1]) << 48
        | (long) (0xff & b[offset]) << 56);
  }

  private static long bytes2LongLittleEndian(final byte[] b, final int offset) {
    return ((0xff & b[offset])
        | (0xff & b[offset + 1]) << 8
        | (0xff & b[offset + 2]) << 16
        | (long) (0xff & b[offset + 3]) << 24
        | (long) (0xff & b[offset + 4]) << 32
        | (long) (0xff & b[offset + 5]) << 40
        | (long) (0xff & b[offset + 6]) << 48
        | (long) (0xff & b[offset + 7]) << 56);
  }
}

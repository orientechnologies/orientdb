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
package com.orientechnologies.orient.core.serialization;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Static helper class to transform any kind of basic data in bytes and vice versa.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OBinaryProtocol {

  public static final int SIZE_BYTE = 1;
  public static final int SIZE_CHAR = 2;
  public static final int SIZE_SHORT = 2;
  public static final int SIZE_INT = 4;
  public static final int SIZE_LONG = 8;

  public static byte[] char2bytes(final char value, final byte[] b, final int iBeginOffset) {
    b[iBeginOffset] = (byte) ((value >>> 8) & 0xFF);
    b[iBeginOffset + 1] = (byte) ((value >>> 0) & 0xFF);
    return b;
  }

  public static int long2bytes(final long value, final OutputStream iStream) throws IOException {
    final int beginOffset =
        iStream instanceof OMemoryStream ? ((OMemoryStream) iStream).getPosition() : -1;

    iStream.write((int) (value >>> 56) & 0xFF);
    iStream.write((int) (value >>> 48) & 0xFF);
    iStream.write((int) (value >>> 40) & 0xFF);
    iStream.write((int) (value >>> 32) & 0xFF);
    iStream.write((int) (value >>> 24) & 0xFF);
    iStream.write((int) (value >>> 16) & 0xFF);
    iStream.write((int) (value >>> 8) & 0xFF);
    iStream.write((int) (value >>> 0) & 0xFF);

    return beginOffset;
  }

  public static byte[] long2bytes(final long value) {
    return OBinaryProtocol.long2bytes(value, new byte[8], 0);
  }

  public static byte[] long2bytes(final long value, final byte[] b, final int iBeginOffset) {
    b[iBeginOffset] = (byte) ((value >>> 56) & 0xFF);
    b[iBeginOffset + 1] = (byte) ((value >>> 48) & 0xFF);
    b[iBeginOffset + 2] = (byte) ((value >>> 40) & 0xFF);
    b[iBeginOffset + 3] = (byte) ((value >>> 32) & 0xFF);
    b[iBeginOffset + 4] = (byte) ((value >>> 24) & 0xFF);
    b[iBeginOffset + 5] = (byte) ((value >>> 16) & 0xFF);
    b[iBeginOffset + 6] = (byte) ((value >>> 8) & 0xFF);
    b[iBeginOffset + 7] = (byte) ((value >>> 0) & 0xFF);
    return b;
  }

  public static int int2bytes(final int value, final OutputStream iStream) throws IOException {
    final int beginOffset =
        iStream instanceof OMemoryStream ? ((OMemoryStream) iStream).getPosition() : -1;

    iStream.write((value >>> 24) & 0xFF);
    iStream.write((value >>> 16) & 0xFF);
    iStream.write((value >>> 8) & 0xFF);
    iStream.write((value >>> 0) & 0xFF);

    return beginOffset;
  }

  public static byte[] int2bytes(final int value) {
    return OBinaryProtocol.int2bytes(value, new byte[4], 0);
  }

  public static byte[] int2bytes(final int value, final byte[] b, final int iBeginOffset) {
    b[iBeginOffset] = (byte) ((value >>> 24) & 0xFF);
    b[iBeginOffset + 1] = (byte) ((value >>> 16) & 0xFF);
    b[iBeginOffset + 2] = (byte) ((value >>> 8) & 0xFF);
    b[iBeginOffset + 3] = (byte) ((value >>> 0) & 0xFF);
    return b;
  }

  public static int short2bytes(final short value, final OutputStream iStream) throws IOException {
    final int beginOffset =
        iStream instanceof OMemoryStream ? ((OMemoryStream) iStream).getPosition() : -1;
    iStream.write((value >>> 8) & 0xFF);
    iStream.write((value >>> 0) & 0xFF);
    return beginOffset;
  }

  public static byte[] short2bytes(final short value) {
    return OBinaryProtocol.short2bytes(value, new byte[2], 0);
  }

  public static byte[] short2bytes(final short value, final byte[] b, final int iBeginOffset) {
    b[iBeginOffset] = (byte) ((value >>> 8) & 0xFF);
    b[iBeginOffset + 1] = (byte) ((value >>> 0) & 0xFF);
    return b;
  }

  public static long bytes2long(final byte[] b) {
    return OBinaryProtocol.bytes2long(b, 0);
  }

  public static long bytes2long(final InputStream iStream) throws IOException {
    return ((long) (0xff & iStream.read()) << 56
        | (long) (0xff & iStream.read()) << 48
        | (long) (0xff & iStream.read()) << 40
        | (long) (0xff & iStream.read()) << 32
        | (long) (0xff & iStream.read()) << 24
        | (0xff & iStream.read()) << 16
        | (0xff & iStream.read()) << 8
        | (0xff & iStream.read()));
  }

  public static long bytes2long(final byte[] b, final int offset) {
    return ((0xff & b[offset + 7])
        | (0xff & b[offset + 6]) << 8
        | (0xff & b[offset + 5]) << 16
        | (long) (0xff & b[offset + 4]) << 24
        | (long) (0xff & b[offset + 3]) << 32
        | (long) (0xff & b[offset + 2]) << 40
        | (long) (0xff & b[offset + 1]) << 48
        | (long) (0xff & b[offset]) << 56);
  }

  /**
   * Convert the byte array to an int.
   *
   * @param b The byte array
   * @return The integer
   */
  public static int bytes2int(final byte[] b) {
    return bytes2int(b, 0);
  }

  public static int bytes2int(final InputStream iStream) throws IOException {
    return ((0xff & iStream.read()) << 24
        | (0xff & iStream.read()) << 16
        | (0xff & iStream.read()) << 8
        | (0xff & iStream.read()));
  }

  /**
   * Convert the byte array to an int starting from the given offset.
   *
   * @param b The byte array
   * @param offset The array offset
   * @return The integer
   */
  public static int bytes2int(final byte[] b, final int offset) {
    return (b[offset]) << 24
        | (0xff & b[offset + 1]) << 16
        | (0xff & b[offset + 2]) << 8
        | ((0xff & b[offset + 3]));
  }

  public static int bytes2short(final InputStream iStream) throws IOException {
    return (short) ((iStream.read() << 8) | (iStream.read() & 0xff));
  }

  public static short bytes2short(final byte[] b, final int offset) {
    return (short) ((b[offset] << 8) | (b[offset + 1] & 0xff));
  }

  public static char bytes2char(final byte[] b, final int offset) {
    return (char) ((b[offset] << 8) + (b[offset + 1] & 0xff));
  }
}

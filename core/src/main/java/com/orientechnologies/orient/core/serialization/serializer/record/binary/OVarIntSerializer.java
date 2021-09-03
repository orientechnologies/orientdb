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

package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OVarIntSerializer {

  public static void write(DataOutput output, long value) throws IOException {
    value = signedToUnsigned(value);
    writeUnsignedVarLong(value, output);
  }

  public static int write(BytesContainer bytes, long value) {
    value = signedToUnsigned(value);
    int pos = bytes.offset;
    writeUnsignedVarLong(value, bytes);
    return pos;
  }

  public static int readAsInt(final DataInput input) throws IOException {
    return (int) readSignedVarLong(input);
  }

  public static long readAsLong(final DataInput input) throws IOException {
    return readSignedVarLong(input);
  }

  public static short readAsShort(final BytesContainer bytes) {
    return (short) readSignedVarLong(bytes);
  }

  public static long readAsLong(final BytesContainer bytes) {
    return readSignedVarLong(bytes);
  }

  public static int readAsInteger(final BytesContainer bytes) {
    return (int) readSignedVarLong(bytes);
  }

  public static byte readAsByte(final BytesContainer bytes) {
    return (byte) readSignedVarLong(bytes);
  }

  /**
   * Encodes a value using the variable-length encoding from <a
   * href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">Google Protocol
   * Buffers</a>. It uses zig-zag encoding to efficiently encode signed values.
   *
   * @param value value to encode
   */
  private static long signedToUnsigned(long value) {
    return (value << 1) ^ (value >> 63);
  }

  /**
   * Encodes a value using the variable-length encoding from <a
   * href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">Google Protocol
   * Buffers</a>. Zig-zag is not used, so input must not be negative.
   *
   * @param value value to encode
   * @return the number of bytes written
   */
  public static void writeUnsignedVarLong(long value, final BytesContainer bos) {
    int pos;
    while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
      // out.writeByte(((int) value & 0x7F) | 0x80);
      pos = bos.alloc((short) 1);
      bos.bytes[pos] = (byte) (value & 0x7F | 0x80);
      value >>>= 7;
    }
    // out.writeByte((int) value & 0x7F);
    pos = bos.alloc((short) 1);
    bos.bytes[pos] = (byte) (value & 0x7F);
  }

  /**
   * Encodes a value using the variable-length encoding from <a
   * href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">Google Protocol
   * Buffers</a>. Zig-zag is not used, so input must not be negative.
   *
   * @param value value to encode
   * @return the number of bytes written
   */
  public static void writeUnsignedVarLong(long value, final DataOutput bos) throws IOException {
    int pos;
    while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
      // out.writeByte(((int) value & 0x7F) | 0x80);
      bos.writeByte((byte) (value & 0x7F | 0x80));
      value >>>= 7;
    }
    // out.writeByte((int) value & 0x7F);
    bos.writeByte((byte) (value & 0x7F));
  }

  /**
   * @param bytes to read bytes from
   * @return decode value
   * @throws IllegalArgumentException if variable-length value does not terminate after 9 bytes have
   *     been read
   */
  public static long readSignedVarLong(final BytesContainer bytes) {
    final long raw = readUnsignedVarLong(bytes);
    // This undoes the trick in writeSignedVarLong()
    final long temp = (((raw << 63) >> 63) ^ raw) >> 1;
    // This extra step lets us deal with the largest signed values by
    // treating
    // negative results from read unsigned methods as like unsigned values
    // Must re-flip the top bit if the original read value had it set.
    return temp ^ (raw & (1L << 63));
  }

  /**
   * @param bytes to read bytes from
   * @return decode value
   * @throws IllegalArgumentException if variable-length value does not terminate after 9 bytes have
   *     been read
   */
  public static long readSignedVarLong(final DataInput bytes) throws IOException {
    final long raw = readUnsignedVarLong(bytes);
    // This undoes the trick in writeSignedVarLong()
    final long temp = (((raw << 63) >> 63) ^ raw) >> 1;
    // This extra step lets us deal with the largest signed values by
    // treating
    // negative results from read unsigned methods as like unsigned values
    // Must re-flip the top bit if the original read value had it set.
    return temp ^ (raw & (1L << 63));
  }

  /**
   * @param bytes to read bytes from
   * @return decode value
   * @throws IllegalArgumentException if variable-length value does not terminate after 9 bytes have
   *     been read
   */
  public static long readUnsignedVarLong(final BytesContainer bytes) {
    long value = 0L;
    int i = 0;
    long b;
    while (((b = bytes.bytes[bytes.offset++]) & 0x80L) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
      if (i > 63)
        throw new IllegalArgumentException("Variable length quantity is too long (must be <= 63)");
    }
    return value | (b << i);
  }

  /**
   * @param bytes to read bytes from
   * @return decode value
   * @throws IllegalArgumentException if variable-length value does not terminate after 9 bytes have
   *     been read
   */
  public static long readUnsignedVarLong(final DataInput bytes) throws IOException {
    long value = 0L;
    int i = 0;
    long b;
    while (((b = bytes.readByte()) & 0x80L) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
      if (i > 63)
        throw new IllegalArgumentException("Variable length quantity is too long (must be <= 63)");
    }
    return value | (b << i);
  }
}

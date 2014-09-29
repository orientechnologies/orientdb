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

package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;

public class OVarIntSerializer {

  public static int write(BytesContainer bytes, long value) {
    value = signedToUnsigned(value);
    int pos = bytes.offset;
    writeUnsignedVarLong(value, bytes);
    return pos;

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

  /**
   * Encodes a value using the variable-length encoding from <a
   * href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>. It uses zig-zag encoding to
   * efficiently encode signed values. If values are known to be nonnegative, {@link #writeUnsignedVarLong(long, DataOutput)} should
   * be used.
   * 
   * @param value
   *          value to encode
   * @param out
   *          to write bytes to
   * @throws IOException
   *           if {@link DataOutput} throws {@link IOException}
   */
  private static long signedToUnsigned(long value) {
    return (value << 1) ^ (value >> 63);
  }

  /**
   * Encodes a value using the variable-length encoding from <a
   * href="http://code.google.com/apis/protocolbuffers/docs/encoding.html"> Google Protocol Buffers</a>. Zig-zag is not used, so
   * input must not be negative. If values can be negative, use {@link #writeSignedVarLong(long, DataOutput)} instead. This method
   * treats negative input as like a large unsigned value.
   * 
   * @param value
   *          value to encode
   * @param out
   *          to write bytes to
   * @return the number of bytes written
   * @throws IOException
   *           if {@link DataOutput} throws {@link IOException}
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
   * @param in
   *          to read bytes from
   * @return decode value
   * @throws IOException
   *           if {@link DataInput} throws {@link IOException}
   * @throws IllegalArgumentException
   *           if variable-length value does not terminate after 9 bytes have been read
   * @see #writeSignedVarLong(long, DataOutput)
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
   * @param in
   *          to read bytes from
   * @return decode value
   * @throws IOException
   *           if {@link DataInput} throws {@link IOException}
   * @throws IllegalArgumentException
   *           if variable-length value does not terminate after 9 bytes have been read
   * @see #writeUnsignedVarLong(long, DataOutput)
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

}

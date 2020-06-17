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

package com.orientechnologies.common.util;

import java.nio.ByteBuffer;

/**
 * This class is utility class for split primitive types to separate byte buffers and vice versa.
 * This class is used because we use many byte buffers for mmap and there is situation when we need
 * to write value on border of two buffers.
 *
 * @author Artem Loginov (logart2007@gmail.com)
 * @since 5/25/12 6:37 AM
 */
public class OByteBufferUtils {
  public static final int SIZE_OF_SHORT = 2;
  public static final int SIZE_OF_INT = 4;
  public static final int SIZE_OF_LONG = 8;
  private static final int SIZE_OF_BYTE_IN_BITS = 8;
  private static final int MASK = 0x000000FF;

  /**
   * Merge short value from two byte buffer. First byte of short will be extracted from first byte
   * buffer and second from second one.
   *
   * @param buffer to read first part of value
   * @param buffer1 to read second part of value
   * @return merged value
   */
  public static short mergeShortFromBuffers(final ByteBuffer buffer, final ByteBuffer buffer1) {
    short result = 0;
    result = (short) (result | (buffer.get() & MASK));
    result = (short) (result << SIZE_OF_BYTE_IN_BITS);
    result = (short) (result | (buffer1.get() & MASK));
    return result;
  }

  /**
   * Merge int value from two byte buffer. First bytes of int will be extracted from first byte
   * buffer and second from second one. How many bytes will be read from first buffer determines
   * based on <code>buffer.remaining()</code> value
   *
   * @param buffer to read first part of value
   * @param buffer1 to read second part of value
   * @return merged value
   */
  public static int mergeIntFromBuffers(final ByteBuffer buffer, final ByteBuffer buffer1) {
    int result = 0;
    final int remaining = buffer.remaining();
    for (int i = 0; i < remaining; ++i) {
      result = result | (buffer.get() & MASK);
      result = result << SIZE_OF_BYTE_IN_BITS;
    }
    for (int i = 0; i < SIZE_OF_INT - remaining - 1; ++i) {
      result = result | (buffer1.get() & MASK);
      result = result << SIZE_OF_BYTE_IN_BITS;
    }
    result = result | (buffer1.get() & MASK);
    return result;
  }

  /**
   * Merge long value from two byte buffer. First bytes of long will be extracted from first byte
   * buffer and second from second one. How many bytes will be read from first buffer determines
   * based on <code>buffer.remaining()</code> value
   *
   * @param buffer to read first part of value
   * @param buffer1 to read second part of value
   * @return merged value
   */
  public static long mergeLongFromBuffers(final ByteBuffer buffer, final ByteBuffer buffer1) {
    long result = 0;
    final int remaining = buffer.remaining();
    for (int i = 0; i < remaining; ++i) {
      result = result | (MASK & buffer.get());
      result = result << SIZE_OF_BYTE_IN_BITS;
    }
    for (int i = 0; i < SIZE_OF_LONG - remaining - 1; ++i) {
      result = result | (MASK & buffer1.get());
      result = result << SIZE_OF_BYTE_IN_BITS;
    }
    result = result | (MASK & buffer1.get());
    return result;
  }

  /**
   * Split short value into two byte buffer. First byte of short will be written to first byte
   * buffer and second to second one.
   *
   * @param buffer to write first part of value
   * @param buffer1 to write second part of value
   */
  public static void splitShortToBuffers(
      final ByteBuffer buffer, final ByteBuffer buffer1, final short iValue) {
    buffer.put((byte) (MASK & (iValue >>> SIZE_OF_BYTE_IN_BITS)));
    buffer1.put((byte) (MASK & iValue));
  }

  /**
   * Split int value into two byte buffer. First byte of int will be written to first byte buffer
   * and second to second one. How many bytes will be written to first buffer determines based on
   * <code>buffer.remaining()</code> value
   *
   * @param buffer to write first part of value
   * @param buffer1 to write second part of value
   */
  public static void splitIntToBuffers(
      final ByteBuffer buffer, final ByteBuffer buffer1, final int iValue) {
    final int remaining = buffer.remaining();
    int i;
    for (i = 0; i < remaining; ++i) {
      buffer.put((byte) (MASK & (iValue >>> SIZE_OF_BYTE_IN_BITS * (SIZE_OF_INT - i - 1))));
    }
    for (int j = 0; j < SIZE_OF_INT - remaining; ++j) {
      buffer1.put((byte) (MASK & (iValue >>> SIZE_OF_BYTE_IN_BITS * (SIZE_OF_INT - i - j - 1))));
    }
  }

  /**
   * Split long value into two byte buffer. First byte of long will be written to first byte buffer
   * and second to second one. How many bytes will be written to first buffer determines based on
   * <code>buffer.remaining()</code> value
   *
   * @param buffer to write first part of value
   * @param buffer1 to write second part of value
   */
  public static void splitLongToBuffers(
      final ByteBuffer buffer, final ByteBuffer buffer1, final long iValue) {
    final int remaining = buffer.remaining();
    int i;
    for (i = 0; i < remaining; ++i) {
      buffer.put((byte) (iValue >> SIZE_OF_BYTE_IN_BITS * (SIZE_OF_LONG - i - 1)));
    }
    for (int j = 0; j < SIZE_OF_LONG - remaining; ++j) {
      buffer1.put((byte) (iValue >> SIZE_OF_BYTE_IN_BITS * (SIZE_OF_LONG - i - j - 1)));
    }
  }
}

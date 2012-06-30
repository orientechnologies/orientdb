/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.common.util;

import java.nio.ByteBuffer;


/**
 * @author Artem Loginov (logart) logart2007@gmail.com Date: 5/25/12 Time: 6:37 AM
 */
public class OByteBufferUtils {
  public static final int  SIZE_OF_SHORT        = 2;
  public static final int  SIZE_OF_INT          = 4;
  public static final int  SIZE_OF_LONG         = 8;
  private static final int SIZE_OF_BYTE_IN_BITS = 8;
  private static final int MASK                 = 0x000000FF;

  public static short mergeShortFromBuffers(ByteBuffer buffer, ByteBuffer buffer1) {
    short result = 0;
    result = (short) (result | (buffer.get() & MASK));
    result = (short) (result << SIZE_OF_BYTE_IN_BITS);
    result = (short) (result | (buffer1.get() & MASK));
    return result;
  }

  public static int mergeIntFromBuffers(ByteBuffer buffer, ByteBuffer buffer1) {
    int result = 0;
    int remaining = buffer.remaining();
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

  public static long mergeLongFromBuffers(ByteBuffer buffer, ByteBuffer buffer1) {
    long result = 0;
    int remaining = buffer.remaining();
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

  public static void splitShortToBuffers(ByteBuffer buffer, ByteBuffer buffer1, short iValue) {
    buffer.put((byte) (MASK & (iValue >>> SIZE_OF_BYTE_IN_BITS)));
    buffer1.put((byte) (MASK & iValue));
  }

  public static void splitIntToBuffers(ByteBuffer buffer, ByteBuffer buffer1, int iValue) {
    int remaining = buffer.remaining();
    int i;
    for (i = 0; i < remaining; ++i) {
      buffer.put((byte) (MASK & (iValue >>> SIZE_OF_BYTE_IN_BITS * (SIZE_OF_INT - i - 1))));
    }
    for (int j = 0; j < SIZE_OF_INT - remaining; ++j) {
      buffer1.put((byte) (MASK & (iValue >>> SIZE_OF_BYTE_IN_BITS * (SIZE_OF_INT - i - j - 1))));
    }
  }

  public static void splitLongToBuffers(ByteBuffer buffer, ByteBuffer buffer1, long iValue) {
    int remaining = buffer.remaining();
    int i;
    for (i = 0; i < remaining; ++i) {
      buffer.put((byte) (iValue >> SIZE_OF_BYTE_IN_BITS * (SIZE_OF_LONG - i - 1)));
    }
    for (int j = 0; j < SIZE_OF_LONG - remaining; ++j) {
      buffer1.put((byte) (iValue >> SIZE_OF_BYTE_IN_BITS * (SIZE_OF_LONG - i - j - 1)));
    }
  }
}

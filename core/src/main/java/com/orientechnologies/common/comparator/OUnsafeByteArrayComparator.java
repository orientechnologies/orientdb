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

package com.orientechnologies.common.comparator;

import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Comparator;
import sun.misc.Unsafe;

/**
 * Comparator for fast byte arrays comparison using {@link Unsafe} class. Bytes are compared like
 * unsigned not like signed bytes.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 08.07.12
 */
@SuppressWarnings("restriction")
public class OUnsafeByteArrayComparator implements Comparator<byte[]> {
  public static final OUnsafeByteArrayComparator INSTANCE = new OUnsafeByteArrayComparator();

  private static final Unsafe unsafe;

  private static final int BYTE_ARRAY_OFFSET;
  private static final boolean littleEndian =
      ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

  private static final int LONG_SIZE = Long.SIZE / Byte.SIZE;

  static {
    unsafe =
        (Unsafe)
            AccessController.doPrivileged(
                (PrivilegedAction<Object>)
                    () -> {
                      try {
                        Field f = Unsafe.class.getDeclaredField("theUnsafe");
                        f.setAccessible(true);
                        return f.get(null);
                      } catch (NoSuchFieldException | IllegalAccessException e) {
                        throw new Error(e);
                      }
                    });

    BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);

    final int byteArrayScale = unsafe.arrayIndexScale(byte[].class);

    if (byteArrayScale != 1) throw new Error();
  }

  public int compare(byte[] arrayOne, byte[] arrayTwo) {
    final int commonLen = Math.min(arrayOne.length, arrayTwo.length);
    final int WORDS = commonLen / LONG_SIZE;

    for (int i = 0; i < WORDS * LONG_SIZE; i += LONG_SIZE) {
      final long index = i + BYTE_ARRAY_OFFSET;

      final long wOne = unsafe.getLong(arrayOne, index);
      final long wTwo = unsafe.getLong(arrayTwo, index);

      if (wOne == wTwo) continue;

      if (littleEndian)
        return lessThanUnsigned(Long.reverseBytes(wOne), Long.reverseBytes(wTwo)) ? -1 : 1;

      return lessThanUnsigned(wOne, wTwo) ? -1 : 1;
    }

    for (int i = WORDS * LONG_SIZE; i < commonLen; i++) {
      int diff = compareUnsignedByte(arrayOne[i], arrayTwo[i]);
      if (diff != 0) return diff;
    }

    return Integer.compare(arrayOne.length, arrayTwo.length);
  }

  private static boolean lessThanUnsigned(long longOne, long longTwo) {
    return (longOne + Long.MIN_VALUE) < (longTwo + Long.MIN_VALUE);
  }

  private static int compareUnsignedByte(byte byteOne, byte byteTwo) {
    final int valOne = byteOne & 0xFF;
    final int valTwo = byteTwo & 0xFF;
    return valOne - valTwo;
  }
}

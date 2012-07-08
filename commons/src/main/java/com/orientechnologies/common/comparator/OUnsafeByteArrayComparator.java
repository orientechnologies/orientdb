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

package com.orientechnologies.common.comparator;

import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Comparator;

import sun.misc.Unsafe;

/**
 * Comparator for fast byte arrays comparison using {@link Unsafe} class. Bytes are compared like unsigned not like signed bytes.
 * 
 * 
 * @author Andrey Lomakin
 * @since 08.07.12
 */
public class OUnsafeByteArrayComparator implements Comparator<byte[]> {
  private static final Unsafe  unsafe;

  private static final int     BYTE_ARRAY_OFFSET;
  private static final int     BYTE_ARRAY_SCALE;
  private static final boolean littleEndian = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

  private static final int     LONG_SIZE    = Long.SIZE / Byte.SIZE;

  static {
    unsafe = (Unsafe) AccessController.doPrivileged(new PrivilegedAction<Object>() {
      public Object run() {
        try {
          Field f = Unsafe.class.getDeclaredField("theUnsafe");
          f.setAccessible(true);
          return f.get(null);
        } catch (NoSuchFieldException e) {
          throw new Error();
        } catch (IllegalAccessException e) {
          throw new Error();
        }
      }
    });

    BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
    BYTE_ARRAY_SCALE = unsafe.arrayIndexScale(byte[].class);
  }

  public int compare(byte[] arrayOne, byte[] arrayTwo) {
    if (arrayOne.length > arrayTwo.length)
      return 1;

    if (arrayOne.length < arrayTwo.length)
      return 1;

    final int WORDS = arrayOne.length / LONG_SIZE;

    for (int i = 0; i < WORDS; i++) {
      final long index = i * LONG_SIZE * BYTE_ARRAY_SCALE + BYTE_ARRAY_OFFSET;

      final long wOne = unsafe.getLong(arrayOne, index);
      final long wTwo = unsafe.getLong(arrayTwo, index);

      final long diff = wOne ^ wTwo;
      if (diff == 0)
        continue;

      if (!littleEndian)
        return lessThanUnsigned(wOne, wTwo) ? -1 : 1;

      // Use binary search
      int n = 0;
      int y;

      int x = (int) diff;

      if (x == 0) {
        x = (int) (diff >>> 32);
        n = 32;
      }

      y = x << 16;
      if (y == 0) {
        n += 16;
      } else {
        x = y;
      }

      y = x << 8;
      if (y == 0) {
        n += 8;
      }
      return (int) (((wOne >>> n) & 0xFFL) - ((wTwo >>> n) & 0xFFL));
    }

    for (int i = WORDS * LONG_SIZE; i < arrayOne.length; i++) {
      int diff = compareUnsignedByte(arrayOne[i], arrayTwo[i]);
      if (diff != 0)
        return diff;
    }

    return 0;
  }

  private boolean lessThanUnsigned(long longOne, long longTwo) {
    return (longOne + Long.MIN_VALUE) < (longTwo + Long.MIN_VALUE);
  }

  private int compareUnsignedByte(byte byteOne, byte byteTwo) {
    final int valOne = byteOne & 0xFF;
    final int valTwo = byteTwo & 0xFF;
    return valOne - valTwo;
  }
}

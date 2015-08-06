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

package com.orientechnologies.common.serialization;

import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import sun.misc.Unsafe;

/**
 * @author Andrey Lomakin
 * @since 26.07.12
 */
@SuppressWarnings("restriction")
public class OUnsafeBinaryConverter implements OBinaryConverter {
  public static final OUnsafeBinaryConverter INSTANCE             = new OUnsafeBinaryConverter();

  private static final Unsafe                theUnsafe;
  private static final long                  BYTE_ARRAY_OFFSET;

  private static final boolean               useOnlyAlignedAccess = OGlobalConfiguration.DIRECT_MEMORY_ONLY_ALIGNED_ACCESS
                                                                      .getValueAsBoolean();

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
          throw new Error(e);
        } catch (IllegalAccessException e) {
          throw new Error(e);
        }
      }
    });
    BYTE_ARRAY_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);
  }

  public void putShort(byte[] buffer, int index, short value, ByteOrder byteOrder) {
    if (!useOnlyAlignedAccess) {
      if (!byteOrder.equals(ByteOrder.nativeOrder()))
        value = Short.reverseBytes(value);

      theUnsafe.putShort(buffer, index + BYTE_ARRAY_OFFSET, value);
    } else {
      if (byteOrder.equals(ByteOrder.BIG_ENDIAN)) {
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET, (byte) (value >>> 8));
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET + 1, (byte) value);
      } else {
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET, (byte) value);
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET + 1, (byte) (value >>> 8));
      }
    }
  }

  public short getShort(byte[] buffer, int index, ByteOrder byteOrder) {
    if (!useOnlyAlignedAccess) {
      short result = theUnsafe.getShort(buffer, index + BYTE_ARRAY_OFFSET);
      if (!byteOrder.equals(ByteOrder.nativeOrder()))
        result = Short.reverseBytes(result);

      return result;
    }

    if (byteOrder.equals(ByteOrder.BIG_ENDIAN))
      return (short) ((theUnsafe.getByte(buffer, index + BYTE_ARRAY_OFFSET)) << 8 | (theUnsafe.getByte(buffer, index
          + BYTE_ARRAY_OFFSET + 1) & 0xff));

    return (short) ((theUnsafe.getByte(buffer, index + BYTE_ARRAY_OFFSET) & 0xff) | (theUnsafe.getByte(buffer, index
        + BYTE_ARRAY_OFFSET + 1) << 8));
  }

  public void putInt(byte[] buffer, int pointer, int value, ByteOrder byteOrder) {
    if (!useOnlyAlignedAccess) {
      final long position = pointer + BYTE_ARRAY_OFFSET;
      if (!byteOrder.equals(ByteOrder.nativeOrder()))
        value = Integer.reverseBytes(value);

      theUnsafe.putInt(buffer, position, value);
    } else {
      if (byteOrder.equals(ByteOrder.BIG_ENDIAN)) {
        theUnsafe.putByte(buffer, pointer + BYTE_ARRAY_OFFSET, (byte) (value >>> 24));
        theUnsafe.putByte(buffer, pointer + BYTE_ARRAY_OFFSET + 1, (byte) (value >>> 16));
        theUnsafe.putByte(buffer, pointer + BYTE_ARRAY_OFFSET + 2, (byte) (value >>> 8));
        theUnsafe.putByte(buffer, pointer + BYTE_ARRAY_OFFSET + 3, (byte) (value));
      } else {
        theUnsafe.putByte(buffer, pointer + BYTE_ARRAY_OFFSET, (byte) (value));
        theUnsafe.putByte(buffer, pointer + BYTE_ARRAY_OFFSET + 1, (byte) (value >>> 8));
        theUnsafe.putByte(buffer, pointer + BYTE_ARRAY_OFFSET + 2, (byte) (value >>> 16));
        theUnsafe.putByte(buffer, pointer + BYTE_ARRAY_OFFSET + 3, (byte) (value >>> 24));
      }
    }
  }

  public int getInt(byte[] buffer, int pointer, ByteOrder byteOrder) {
    if (!useOnlyAlignedAccess) {
      final long position = pointer + BYTE_ARRAY_OFFSET;
      int result = theUnsafe.getInt(buffer, position);
      if (!byteOrder.equals(ByteOrder.nativeOrder()))
        result = Integer.reverseBytes(result);

      return result;
    }

    if (byteOrder.equals(ByteOrder.BIG_ENDIAN)) {
      return ((0xFF & theUnsafe.getByte(buffer, pointer + BYTE_ARRAY_OFFSET)) << 24)
          | ((0xFF & theUnsafe.getByte(buffer, pointer + BYTE_ARRAY_OFFSET + 1)) << 16)
          | ((0xFF & theUnsafe.getByte(buffer, pointer + BYTE_ARRAY_OFFSET + 2)) << 8)
          | (0xFF & theUnsafe.getByte(buffer, pointer + BYTE_ARRAY_OFFSET + 3));
    }

    return (0xFF & theUnsafe.getByte(buffer, pointer + BYTE_ARRAY_OFFSET))
        | ((0xFF & theUnsafe.getByte(buffer, pointer + BYTE_ARRAY_OFFSET + 1)) << 8)
        | ((0xFF & theUnsafe.getByte(buffer, pointer + BYTE_ARRAY_OFFSET + 2)) << 16)
        | ((0xFF & theUnsafe.getByte(buffer, pointer + BYTE_ARRAY_OFFSET + 3)) << 24);
  }

  public void putLong(byte[] buffer, int index, long value, ByteOrder byteOrder) {
    if (!useOnlyAlignedAccess) {
      if (!byteOrder.equals(ByteOrder.nativeOrder()))
        value = Long.reverseBytes(value);

      theUnsafe.putLong(buffer, index + BYTE_ARRAY_OFFSET, value);
    } else {
      if (byteOrder.equals(ByteOrder.BIG_ENDIAN)) {
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET, (byte) (value >>> 56));
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET + 1, (byte) (value >>> 48));
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET + 2, (byte) (value >>> 40));
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET + 3, (byte) (value >>> 32));
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET + 4, (byte) (value >>> 24));
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET + 5, (byte) (value >>> 16));
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET + 6, (byte) (value >>> 8));
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET + 7, (byte) (value));
      } else {
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET, (byte) (value));
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET + 1, (byte) (value >>> 8));
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET + 2, (byte) (value >>> 16));
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET + 3, (byte) (value >>> 24));
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET + 4, (byte) (value >>> 32));
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET + 5, (byte) (value >>> 40));
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET + 6, (byte) (value >>> 48));
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET + 7, (byte) (value >>> 56));
      }
    }
  }

  public long getLong(byte[] buffer, int index, ByteOrder byteOrder) {
    if (!useOnlyAlignedAccess) {
      long result = theUnsafe.getLong(buffer, index + BYTE_ARRAY_OFFSET);
      if (!byteOrder.equals(ByteOrder.nativeOrder()))
        result = Long.reverseBytes(result);

      return result;
    }

    if (byteOrder.equals(ByteOrder.BIG_ENDIAN))
      return ((0xFFL & theUnsafe.getByte(buffer, index + BYTE_ARRAY_OFFSET)) << 56)
          | ((0xFFL & theUnsafe.getByte(buffer, index + BYTE_ARRAY_OFFSET + 1)) << 48)
          | ((0xFFL & theUnsafe.getByte(buffer, index + BYTE_ARRAY_OFFSET + 2)) << 40)
          | ((0xFFL & theUnsafe.getByte(buffer, index + BYTE_ARRAY_OFFSET + 3)) << 32)
          | ((0xFFL & theUnsafe.getByte(buffer, index + BYTE_ARRAY_OFFSET + 4)) << 24)
          | ((0xFFL & theUnsafe.getByte(buffer, index + BYTE_ARRAY_OFFSET + 5)) << 16)
          | ((0xFFL & theUnsafe.getByte(buffer, index + BYTE_ARRAY_OFFSET + 6)) << 8)
          | (0xFFL & theUnsafe.getByte(buffer, index + BYTE_ARRAY_OFFSET + 7));

    return (0xFFL & theUnsafe.getByte(buffer, index + BYTE_ARRAY_OFFSET))
        | ((0xFFL & theUnsafe.getByte(buffer, index + BYTE_ARRAY_OFFSET + 1)) << 8)
        | ((0xFFL & theUnsafe.getByte(buffer, index + BYTE_ARRAY_OFFSET + 2)) << 16)
        | ((0xFFL & theUnsafe.getByte(buffer, index + BYTE_ARRAY_OFFSET + 3)) << 24)
        | ((0xFFL & theUnsafe.getByte(buffer, index + BYTE_ARRAY_OFFSET + 4)) << 32)
        | ((0xFFL & theUnsafe.getByte(buffer, index + BYTE_ARRAY_OFFSET + 5)) << 40)
        | ((0xFFL & theUnsafe.getByte(buffer, index + BYTE_ARRAY_OFFSET + 6)) << 48)
        | ((0xFFL & theUnsafe.getByte(buffer, index + BYTE_ARRAY_OFFSET + 7)) << 56);
  }

  public void putChar(byte[] buffer, int index, char character, ByteOrder byteOrder) {
    if (!useOnlyAlignedAccess) {
      if (!byteOrder.equals(ByteOrder.nativeOrder()))
        character = Character.reverseBytes(character);

      theUnsafe.putChar(buffer, index + BYTE_ARRAY_OFFSET, character);
    } else {
      if (byteOrder.equals(ByteOrder.BIG_ENDIAN)) {
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET, (byte) (character >>> 8));
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET + 1, (byte) (character));
      } else {
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET, (byte) (character));
        theUnsafe.putByte(buffer, index + BYTE_ARRAY_OFFSET + 1, (byte) (character >>> 8));
      }

    }
  }

  public char getChar(byte[] buffer, int index, ByteOrder byteOrder) {
    if (!useOnlyAlignedAccess) {
      char result = theUnsafe.getChar(buffer, index + BYTE_ARRAY_OFFSET);
      if (!byteOrder.equals(ByteOrder.nativeOrder()))
        result = Character.reverseBytes(result);

      return result;
    }

    if (byteOrder.equals(ByteOrder.BIG_ENDIAN))
      return (char) ((theUnsafe.getByte(buffer, index + BYTE_ARRAY_OFFSET) << 8) | (theUnsafe.getByte(buffer, index
          + BYTE_ARRAY_OFFSET + 1) & 0xff));

    return (char) ((theUnsafe.getByte(buffer, index + BYTE_ARRAY_OFFSET) & 0xff) | (theUnsafe.getByte(buffer, index
        + BYTE_ARRAY_OFFSET + 1) << 8));
  }

  public boolean nativeAccelerationUsed() {
    return true;
  }
}

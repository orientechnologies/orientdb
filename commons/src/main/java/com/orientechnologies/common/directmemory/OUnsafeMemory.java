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
package com.orientechnologies.common.directmemory;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

import sun.misc.Unsafe;

import com.orientechnologies.common.serialization.types.OBinarySerializer;

/**
 * @author Andrey Lomakin
 * @since 2/4/13
 */
@SuppressWarnings("restriction")
public class OUnsafeMemory implements ODirectMemory {
  public static final OUnsafeMemory INSTANCE;

  protected static final Unsafe       unsafe;
  private static final boolean      unaligned;

  private static final long         UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

  static {
    OUnsafeMemory futureInstance;
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

    try {
      unsafe.getClass().getDeclaredMethod("copyMemory", Object.class, long.class, Object.class, long.class, long.class);
      Class<?> unsafeMemoryJava7 = OUnsafeMemory.class.getClassLoader().loadClass("com.orientechnologies.common.directmemory.OUnsafeMemoryJava7");
      futureInstance = (OUnsafeMemory) unsafeMemoryJava7.newInstance();
    } catch (Exception e) {
      futureInstance = new OUnsafeMemory();
    }

    INSTANCE = futureInstance;
    String arch = System.getProperty("os.arch");
    unaligned = arch.equals("i386") || arch.equals("x86") || arch.equals("amd64") || arch.equals("x86_64");
  }

  @Override
  public long allocate(byte[] bytes) {
    final long pointer = unsafe.allocateMemory(bytes.length);
    set(pointer, bytes, bytes.length);

    return pointer;
  }

  @Override
  public long allocate(long size) {
    return unsafe.allocateMemory(size);
  }

  @Override
  public void free(long pointer) {
    unsafe.freeMemory(pointer);
  }

  @Override
  public byte[] get(long pointer, final int length) {
    final byte[] result = new byte[length];

    for (int i = 0; i < length; i++)
      result[i] = unsafe.getByte(pointer++);

    return result;
  }

  @Override
  public void get(long pointer, byte[] array, int arrayOffset, int length) {
    pointer += arrayOffset;
    for (int i = arrayOffset; i < length + arrayOffset; i++)
      array[i] = unsafe.getByte(pointer++);

  }

  @Override
  public void set(long pointer, byte[] content, int length) {
    for (int i = 0; i < length; i++)
      unsafe.putByte(pointer++, content[i]);
  }

  @Override
  public <T> T get(long pointer, OBinarySerializer<T> serializer) {
    return serializer.deserializeFromDirectMemory(this, pointer);
  }

  @Override
  public <T> void set(long pointer, T data, OBinarySerializer<T> serializer) {
    serializer.serializeInDirectMemory(data, this, pointer);
  }

  @Override
  public int getInt(long pointer) {
    if (unaligned)
      return unsafe.getInt(pointer);

    return (0xFF & unsafe.getByte(pointer++)) << 24 | (0xFF & unsafe.getByte(pointer++)) << 16
        | (0xFF & unsafe.getByte(pointer++)) << 8 | (0xFF & unsafe.getByte(pointer));
  }

  @Override
  public void setInt(long pointer, int value) {
    if (unaligned)
      unsafe.putInt(pointer, value);
    else {
      unsafe.putByte(pointer++, (byte) (value >>> 24));
      unsafe.putByte(pointer++, (byte) (value >>> 16));
      unsafe.putByte(pointer++, (byte) (value >>> 8));
      unsafe.putByte(pointer, (byte) (value));
    }
  }

  @Override
  public void setShort(long pointer, short value) {
    if (unaligned)
      unsafe.putShort(pointer, value);
    else {
      unsafe.putByte(pointer++, (byte) (value >>> 8));
      unsafe.putByte(pointer, (byte) value);
    }
  }

  @Override
  public short getShort(long pointer) {
    if (unaligned)
      return unsafe.getShort(pointer);

    return (short) (unsafe.getByte(pointer++) << 8 | (unsafe.getByte(pointer) & 0xff));
  }

  @Override
  public void setChar(long pointer, char value) {
    if (unaligned)
      unsafe.putChar(pointer, value);
    else {
      unsafe.putByte(pointer++, (byte) (value >>> 8));
      unsafe.putByte(pointer, (byte) (value));
    }
  }

  @Override
  public char getChar(long pointer) {
    if (unaligned)
      return unsafe.getChar(pointer);

    return (char) ((unsafe.getByte(pointer++) << 8) | (unsafe.getByte(pointer) & 0xff));
  }

  @Override
  public long getLong(long pointer) {
    if (unaligned)
      return unsafe.getLong(pointer);

    return (0xFFL & unsafe.getByte(pointer++)) << 56 | (0xFFL & unsafe.getByte(pointer++)) << 48
        | (0xFFL & unsafe.getByte(pointer++)) << 40 | (0xFFL & unsafe.getByte(pointer++)) << 32
        | (0xFFL & unsafe.getByte(pointer++)) << 24 | (0xFFL & unsafe.getByte(pointer++)) << 16
        | (0xFFL & unsafe.getByte(pointer++)) << 8 | (0xFFL & unsafe.getByte(pointer));

  }

  @Override
  public void setLong(long pointer, long value) {
    if (unaligned)
      unsafe.putLong(pointer, value);
    else {
      unsafe.putByte(pointer++, (byte) (value >>> 56));
      unsafe.putByte(pointer++, (byte) (value >>> 48));
      unsafe.putByte(pointer++, (byte) (value >>> 40));
      unsafe.putByte(pointer++, (byte) (value >>> 32));
      unsafe.putByte(pointer++, (byte) (value >>> 24));
      unsafe.putByte(pointer++, (byte) (value >>> 16));
      unsafe.putByte(pointer++, (byte) (value >>> 8));
      unsafe.putByte(pointer, (byte) (value));
    }
  }

  @Override
  public byte getByte(long pointer) {
    return unsafe.getByte(pointer);
  }

  @Override
  public void setByte(long pointer, byte value) {
    unsafe.putByte(pointer, value);
  }

  @Override
  public void copyData(long srcPointer, long destPointer, long len) {
    while (len > 0) {
      long size = (len > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : len;
      unsafe.copyMemory(srcPointer, destPointer, size);

      len -= size;
      srcPointer += size;
      destPointer += size;
    }
  }
}

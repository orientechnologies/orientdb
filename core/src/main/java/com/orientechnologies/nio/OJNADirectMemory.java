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

package com.orientechnologies.nio;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

import java.nio.ByteOrder;

/**
 * @author Andrey Lomakin
 * @since 5/6/13
 */
public class OJNADirectMemory implements ODirectMemory {
  private static final CLibrary C_LIBRARY = OCLibraryFactory.INSTANCE.library();

  public static final OJNADirectMemory INSTANCE = new OJNADirectMemory();

  private static final boolean   unaligned;
  private static final ByteOrder alignedOrder;
  private static final boolean onlyAlignedOrder = OGlobalConfiguration.DIRECT_MEMORY_ONLY_ALIGNED_ACCESS.getValueAsBoolean();

  static {
    alignedOrder = ByteOrder.nativeOrder();
    unaligned = !onlyAlignedOrder;
  }

  @Override
  public long allocate(long size) {
    final long pointer = Native.malloc(size);
    if (pointer == 0)
      throw new OutOfMemoryError();

    return pointer;
  }

  @Override
  public void free(long pointer) {
    Native.free(pointer);
  }

  @Override
  public byte[] get(long pointer, int length) {
    return new Pointer(pointer).getByteArray(0, length);
  }

  @Override
  public void get(long pointer, byte[] array, int arrayOffset, int length) {
    new Pointer(pointer).read(0, array, arrayOffset, length);
  }

  @Override
  public void set(long pointer, byte[] content, int arrayOffset, int length) {
    new Pointer(pointer).write(0, content, arrayOffset, length);
  }

  @Override
  public int getInt(long pointer) {
    final Pointer ptr = new Pointer(pointer);
    if (unaligned)
      return ptr.getInt(0);

    if (alignedOrder.equals(ByteOrder.BIG_ENDIAN))
      return (0xFF & ptr.getByte(0)) << 24 | (0xFF & ptr.getByte(1)) << 16 | (0xFF & ptr.getByte(2)) << 8 | (0xFF & ptr.getByte(3));

    return (0xFF & ptr.getByte(0)) | (0xFF & ptr.getByte(1)) << 8 | (0xFF & ptr.getByte(2)) << 16 | (0xFF & ptr.getByte(3)) << 24;
  }

  @Override
  public void setInt(long pointer, int value) {
    final Pointer ptr = new Pointer(pointer);
    if (unaligned)
      ptr.setInt(0, value);
    else {
      if (alignedOrder.equals(ByteOrder.BIG_ENDIAN)) {
        ptr.setByte(0, (byte) (value >>> 24));
        ptr.setByte(1, (byte) (value >>> 16));
        ptr.setByte(2, (byte) (value >>> 8));
        ptr.setByte(3, (byte) (value));
      } else {
        ptr.setByte(0, (byte) (value));
        ptr.setByte(1, (byte) (value >>> 8));
        ptr.setByte(2, (byte) (value >>> 16));
        ptr.setByte(3, (byte) (value >>> 24));
      }
    }

  }

  @Override
  public void setShort(long pointer, short value) {
    final Pointer ptr = new Pointer(pointer);
    if (unaligned)
      ptr.setShort(0, value);
    else {
      if (alignedOrder.equals(ByteOrder.BIG_ENDIAN)) {
        ptr.setByte(0, (byte) (value >>> 8));
        ptr.setByte(1, (byte) value);
      } else {
        ptr.setByte(0, (byte) value);
        ptr.setByte(1, (byte) (value >>> 8));
      }
    }
  }

  @Override
  public short getShort(long pointer) {
    final Pointer ptr = new Pointer(pointer);
    if (unaligned)
      return ptr.getShort(0);

    if (alignedOrder.equals(ByteOrder.BIG_ENDIAN))
      return (short) (ptr.getByte(0) << 8 | (ptr.getByte(1) & 0xff));

    return (short) ((ptr.getByte(0) & 0xff) | (ptr.getByte(1) << 8));
  }

  @Override
  public long getLong(long pointer) {
    final Pointer ptr = new Pointer(pointer);
    if (unaligned)
      return ptr.getLong(0);

    if (alignedOrder.equals(ByteOrder.BIG_ENDIAN))
      return (0xFFL & ptr.getByte(0)) << 56 | (0xFFL & ptr.getByte(1)) << 48 | (0xFFL & ptr.getByte(2)) << 40
          | (0xFFL & ptr.getByte(3)) << 32 | (0xFFL & ptr.getByte(4)) << 24 | (0xFFL & ptr.getByte(5)) << 16
          | (0xFFL & ptr.getByte(6)) << 8 | (0xFFL & ptr.getByte(7));

    return (0xFFL & ptr.getByte(0)) | (0xFFL & ptr.getByte(1)) << 8 | (0xFFL & ptr.getByte(2)) << 16
        | (0xFFL & ptr.getByte(3)) << 24 | (0xFFL & ptr.getByte(4)) << 32 | (0xFFL & ptr.getByte(5)) << 40
        | (0xFFL & ptr.getByte(6)) << 48 | (0xFFL & ptr.getByte(7)) << 56;
  }

  @Override
  public void setLong(long pointer, long value) {
    final Pointer ptr = new Pointer(pointer);

    if (unaligned)
      ptr.setLong(0, value);
    else {
      if (alignedOrder.equals(ByteOrder.BIG_ENDIAN)) {
        ptr.setByte(0, (byte) (value >>> 56));
        ptr.setByte(1, (byte) (value >>> 48));
        ptr.setByte(2, (byte) (value >>> 40));
        ptr.setByte(3, (byte) (value >>> 32));
        ptr.setByte(4, (byte) (value >>> 24));
        ptr.setByte(5, (byte) (value >>> 16));
        ptr.setByte(6, (byte) (value >>> 8));
        ptr.setByte(7, (byte) (value));
      } else {
        ptr.setByte(0, (byte) (value));
        ptr.setByte(1, (byte) (value >>> 8));
        ptr.setByte(2, (byte) (value >>> 16));
        ptr.setByte(3, (byte) (value >>> 24));
        ptr.setByte(4, (byte) (value >>> 32));
        ptr.setByte(5, (byte) (value >>> 40));
        ptr.setByte(6, (byte) (value >>> 48));
        ptr.setByte(7, (byte) (value >>> 56));
      }
    }
  }

  @Override
  public byte getByte(long pointer) {
    return new Pointer(pointer).getByte(0);
  }

  @Override
  public void setByte(long pointer, byte value) {
    new Pointer(pointer).setByte(0, value);
  }

  @Override
  public void setChar(long pointer, char value) {
    final short short_char = (short) value;
    setShort(pointer, short_char);
  }

  @Override
  public char getChar(long pointer) {
    final short short_char = getShort(pointer);
    return (char) short_char;
  }

  @Override
  public void moveData(long srcPointer, long destPointer, long len) {
    C_LIBRARY.memoryMove(srcPointer, destPointer, len);
  }
}

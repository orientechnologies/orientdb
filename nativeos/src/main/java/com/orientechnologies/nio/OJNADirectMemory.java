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

package com.orientechnologies.nio;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

/**
 * @author Andrey Lomakin
 * @since 5/6/13
 */
public class OJNADirectMemory implements ODirectMemory {
  public static final OJNADirectMemory INSTANCE = new OJNADirectMemory();

  @Override
  public long allocate(byte[] bytes) {
    final long pointer = allocate(bytes.length);
    set(pointer, bytes, 0, bytes.length);

    return pointer;
  }

  @Override
  public long allocate(long size) {
    return Native.malloc(size);
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
    return new Pointer(pointer).getInt(0);
  }

  @Override
  public void setInt(long pointer, int value) {
    new Pointer(pointer).setInt(0, value);
  }

  @Override
  public void setShort(long pointer, short value) {
    new Pointer(pointer).setShort(0, value);
  }

  @Override
  public short getShort(long pointer) {
    return new Pointer(pointer).getShort(0);
  }

  @Override
  public long getLong(long pointer) {
    return new Pointer(pointer).getLong(0);
  }

  @Override
  public void setLong(long pointer, long value) {
    new Pointer(pointer).setLong(0, value);
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
    new Pointer(pointer).setShort(0, short_char);
  }

  @Override
  public char getChar(long pointer) {
    final short short_char = new Pointer(pointer).getShort(0);
    return (char) short_char;
  }

  @Override
  public void copyData(long srcPointer, long destPointer, long len) {
    CLibrary.memoryMove(srcPointer, destPointer, len);
  }

}

/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

package com.orientechnologies.common.serialization.types;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;

/**
 * Serializer for byte type .
 * 
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OByteSerializer implements OBinarySerializer<Byte> {
  /**
   * size of byte value in bytes
   */
  public static final int       BYTE_SIZE = 1;

  public static OByteSerializer INSTANCE  = new OByteSerializer();
  public static final byte      ID        = 2;

  public int getObjectSize(Byte object, Object... hints) {
    return BYTE_SIZE;
  }

  public void serialize(Byte object, byte[] stream, int startPosition, Object... hints) {
    stream[startPosition] = object;
  }

  public Byte deserialize(byte[] stream, int startPosition) {
    return stream[startPosition];
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return BYTE_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return getObjectSize(stream, startPosition);
  }

  public void serializeNative(Byte object, byte[] stream, int startPosition, Object... hints) {
    serialize(object, stream, startPosition);
  }

  public Byte deserializeNative(byte[] stream, int startPosition) {
    return deserialize(stream, startPosition);
  }

  @Override
  public void serializeInDirectMemory(Byte object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    pointer.setByte(offset, object);
  }

  @Override
  public Byte deserializeFromDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return pointer.getByte(offset);
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return BYTE_SIZE;
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return BYTE_SIZE;
  }

  @Override
  public Byte prepocess(Byte value, Object... hints) {
    return value;
  }
}

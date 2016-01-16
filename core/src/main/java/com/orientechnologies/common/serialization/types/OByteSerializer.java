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

package com.orientechnologies.common.serialization.types;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;

/**
 * Serializer for byte type .
 *
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 18.01.12
 */
public class OByteSerializer implements OBinarySerializer<Byte> {
  /**
   * size of byte value in bytes
   */
  public static final int       BYTE_SIZE = 1;
  public static final byte      ID        = 2;
  public static OByteSerializer INSTANCE  = new OByteSerializer();

  public int getObjectSize(Byte object, Object... hints) {
    return BYTE_SIZE;
  }

  public void serialize(final Byte object, final byte[] stream, final int startPosition, final Object... hints) {
    stream[startPosition] = object.byteValue();
  }

  public void serializeLiteral(final byte value, final byte[] stream, final int startPosition) {
    stream[startPosition] = value;
  }

  public Byte deserialize(final byte[] stream, final int startPosition) {
    return stream[startPosition];
  }

  public byte deserializeLiteral(final byte[] stream, final int startPosition) {
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

  @Override
  public void serializeNativeObject(final Byte object, final byte[] stream, final int startPosition, final Object... hints) {
    serialize(object, stream, startPosition);
  }

  public void serializeNative(byte object, byte[] stream, int startPosition, Object... hints) {
    serializeLiteral(object, stream, startPosition);
  }

  @Override
  public Byte deserializeNativeObject(final byte[] stream, final int startPosition) {
    return stream[startPosition];
  }

  public byte deserializeNative(final byte[] stream, final int startPosition) {
    return stream[startPosition];
  }

  @Override
  public void serializeInDirectMemoryObject(final Byte object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    pointer.setByte(offset, object);
  }

  public void serializeInDirectMemory(final byte object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    pointer.setByte(offset, object);
  }

  @Override
  public Byte deserializeFromDirectMemoryObject(final ODirectMemoryPointer pointer, final long offset) {
    return pointer.getByte(offset);
  }

  @Override
  public Byte deserializeFromDirectMemoryObject(OWALChangesTree.PointerWrapper wrapper, long offset) {
    return wrapper.getByte(offset);
  }

  public byte deserializeFromDirectMemory(final ODirectMemoryPointer pointer, final long offset) {
    return pointer.getByte(offset);
  }

  public byte deserializeFromDirectMemory(final OWALChangesTree.PointerWrapper wrapper, final long offset) {
    return wrapper.getByte(offset);
  }

  @Override
  public int getObjectSizeInDirectMemory(final ODirectMemoryPointer pointer, final long offset) {
    return BYTE_SIZE;
  }

  @Override
  public int getObjectSizeInDirectMemory(OWALChangesTree.PointerWrapper wrapper, long offset) {
    return BYTE_SIZE;
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return BYTE_SIZE;
  }

  @Override
  public Byte preprocess(Byte value, Object... hints) {
    return value;
  }
}

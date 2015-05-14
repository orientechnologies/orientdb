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
 * Serializer for boolean type .
 *
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 18.01.12
 */
public class OBooleanSerializer implements OBinarySerializer<Boolean> {
  /**
   * size of boolean value in bytes
   */
  public static final int          BOOLEAN_SIZE = 1;
  public static final byte         ID           = 1;
  public static OBooleanSerializer INSTANCE     = new OBooleanSerializer();

  public int getObjectSize(Boolean object, Object... hints) {
    return BOOLEAN_SIZE;
  }

  public void serialize(final Boolean object, final byte[] stream, final int startPosition, final Object... hints) {
    stream[startPosition] = object ? (byte) 1 : (byte) 0;
  }

  public void serializeLiteral(final boolean value, final byte[] stream, final int startPosition) {
    stream[startPosition] = value ? (byte) 1 : (byte) 0;
  }

  public Boolean deserialize(final byte[] stream, final int startPosition) {
    return stream[startPosition] == 1;
  }

  public boolean deserializeLiteral(final byte[] stream, final int startPosition) {
    return stream[startPosition] == 1;
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return BOOLEAN_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return BOOLEAN_SIZE;
  }

  @Override
  public void serializeNativeObject(final Boolean object, final byte[] stream, final int startPosition, final Object... hints) {
    serialize(object, stream, startPosition);
  }

  public void serializeNative(final boolean object, final byte[] stream, final int startPosition, final Object... hints) {
    serializeLiteral(object, stream, startPosition);
  }

  @Override
  public Boolean deserializeNativeObject(final byte[] stream, final int startPosition) {
    return deserialize(stream, startPosition);
  }

  public boolean deserializeNative(final byte[] stream, final int startPosition) {
    return deserializeLiteral(stream, startPosition);
  }

  public void serializeInDirectMemory(Boolean object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    pointer.setByte(offset, object ? (byte) 1 : 0);
  }

  @Override
  public void serializeInDirectMemoryObject(final Boolean object, final ODirectMemoryPointer pointer, final long offset,
      final Object... hints) {
    pointer.setByte(offset, object.booleanValue() ? (byte) 1 : 0);
  }

  public void serializeInDirectMemory(final boolean object, final ODirectMemoryPointer pointer, final long offset,
      final Object... hints) {
    pointer.setByte(offset, object ? (byte) 1 : 0);
  }

  @Override
  public Boolean deserializeFromDirectMemoryObject(final ODirectMemoryPointer pointer, final long offset) {
    return pointer.getByte(offset) > 0;
  }

  @Override
  public Boolean deserializeFromDirectMemoryObject(OWALChangesTree.PointerWrapper wrapper, long offset) {
    return wrapper.getByte(offset) > 0;
  }

  public boolean deserializeFromDirectMemory(final ODirectMemoryPointer pointer, final long offset) {
    return pointer.getByte(offset) > 0;
  }

  public boolean deserializeFromDirectMemory(OWALChangesTree.PointerWrapper wrapper, long offset) {
    return wrapper.getByte(offset) > 0;
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return BOOLEAN_SIZE;
  }

  @Override
  public int getObjectSizeInDirectMemory(OWALChangesTree.PointerWrapper wrapper, long offset) {
    return BOOLEAN_SIZE;
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return BOOLEAN_SIZE;
  }

  @Override
  public Boolean preprocess(final Boolean value, final Object... hints) {
    return value;
  }
}

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

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Serializer for {@link BigDecimal} type.
 *
 * @author Andrey Lomakin
 * @since 03.04.12
 */
public class ODecimalSerializer implements OBinarySerializer<BigDecimal> {
  public static final ODecimalSerializer INSTANCE = new ODecimalSerializer();
  public static final byte               ID       = 18;

  public int getObjectSize(BigDecimal object, Object... hints) {
    return OIntegerSerializer.INT_SIZE + OBinaryTypeSerializer.INSTANCE.getObjectSize(object.unscaledValue().toByteArray());
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    final int size = OIntegerSerializer.INT_SIZE
        + OBinaryTypeSerializer.INSTANCE.getObjectSize(stream, startPosition + OIntegerSerializer.INT_SIZE);
    return size;
  }

  public void serialize(BigDecimal object, byte[] stream, int startPosition, Object... hints) {
    OIntegerSerializer.INSTANCE.serializeLiteral(object.scale(), stream, startPosition);
    startPosition += OIntegerSerializer.INT_SIZE;
    OBinaryTypeSerializer.INSTANCE.serialize(object.unscaledValue().toByteArray(), stream, startPosition);

  }

  public BigDecimal deserialize(final byte[] stream, int startPosition) {
    final int scale = OIntegerSerializer.INSTANCE.deserializeLiteral(stream, startPosition);
    startPosition += OIntegerSerializer.INT_SIZE;

    final byte[] unscaledValue = OBinaryTypeSerializer.INSTANCE.deserialize(stream, startPosition);

    return new BigDecimal(new BigInteger(unscaledValue), scale);
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(final byte[] stream, final int startPosition) {
    final int size = OIntegerSerializer.INT_SIZE
        + OBinaryTypeSerializer.INSTANCE.getObjectSizeNative(stream, startPosition + OIntegerSerializer.INT_SIZE);
    return size;
  }

  @Override
  public void serializeNativeObject(BigDecimal object, byte[] stream, int startPosition, Object... hints) {
    OIntegerSerializer.INSTANCE.serializeNative(object.scale(), stream, startPosition);
    startPosition += OIntegerSerializer.INT_SIZE;
    OBinaryTypeSerializer.INSTANCE.serializeNativeObject(object.unscaledValue().toByteArray(), stream, startPosition);
  }

  @Override
  public BigDecimal deserializeNativeObject(byte[] stream, int startPosition) {
    final int scale = OIntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);
    startPosition += OIntegerSerializer.INT_SIZE;

    final byte[] unscaledValue = OBinaryTypeSerializer.INSTANCE.deserializeNativeObject(stream, startPosition);

    return new BigDecimal(new BigInteger(unscaledValue), scale);
  }

  @Override
  public void serializeInDirectMemoryObject(BigDecimal object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    OIntegerSerializer.INSTANCE.serializeInDirectMemory(object.scale(), pointer, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OBinaryTypeSerializer.INSTANCE.serializeInDirectMemoryObject(object.unscaledValue().toByteArray(), pointer, offset);
  }

  @Override
  public BigDecimal deserializeFromDirectMemoryObject(ODirectMemoryPointer pointer, long offset) {
    final int scale = pointer.getInt(offset);
    offset += OIntegerSerializer.INT_SIZE;

    final byte[] unscaledValue = OBinaryTypeSerializer.INSTANCE.deserializeFromDirectMemoryObject(pointer, offset);

    return new BigDecimal(new BigInteger(unscaledValue), scale);
  }

  @Override
  public BigDecimal deserializeFromDirectMemoryObject(OWALChangesTree.PointerWrapper wrapper, long offset) {
    final int scale = wrapper.getInt(offset);
    offset += OIntegerSerializer.INT_SIZE;

    final byte[] unscaledValue = OBinaryTypeSerializer.INSTANCE.deserializeFromDirectMemoryObject(wrapper, offset);

    return new BigDecimal(new BigInteger(unscaledValue), scale);
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    final int size = OIntegerSerializer.INT_SIZE
        + OBinaryTypeSerializer.INSTANCE.getObjectSizeInDirectMemory(pointer, offset + OIntegerSerializer.INT_SIZE);
    return size;
  }

  @Override
  public int getObjectSizeInDirectMemory(OWALChangesTree.PointerWrapper wrapper, long offset) {
    final int size = OIntegerSerializer.INT_SIZE
        + OBinaryTypeSerializer.INSTANCE.getObjectSizeInDirectMemory(wrapper, offset + OIntegerSerializer.INT_SIZE);
    return size;
  }

  public boolean isFixedLength() {
    return false;
  }

  public int getFixedLength() {
    return 0;
  }

  @Override
  public BigDecimal preprocess(BigDecimal value, Object... hints) {
    return value;
  }
}

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
import com.orientechnologies.common.serialization.OBinaryConverter;
import com.orientechnologies.common.serialization.OBinaryConverterFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;

import java.nio.ByteOrder;

/**
 * Serializer for {@link Float} type.
 *
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 18.01.12
 */
public class OFloatSerializer implements OBinarySerializer<Float> {
  public static final byte              ID         = 7;
  /**
   * size of float value in bytes
   */
  public static final int               FLOAT_SIZE = 4;
  private static final OBinaryConverter CONVERTER  = OBinaryConverterFactory.getConverter();
  public static OFloatSerializer        INSTANCE   = new OFloatSerializer();

  public int getObjectSize(Float object, Object... hints) {
    return FLOAT_SIZE;
  }

  public void serialize(Float object, byte[] stream, int startPosition, Object... hints) {
    OIntegerSerializer.INSTANCE.serializeLiteral(Float.floatToIntBits(object), stream, startPosition);
  }

  public Float deserialize(final byte[] stream, final int startPosition) {
    return Float.intBitsToFloat(OIntegerSerializer.INSTANCE.deserializeLiteral(stream, startPosition));
  }

  public int getObjectSize(final byte[] stream, final int startPosition) {
    return FLOAT_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return FLOAT_SIZE;
  }

  @Override
  public void serializeNativeObject(Float object, byte[] stream, int startPosition, Object... hints) {
    CONVERTER.putInt(stream, startPosition, Float.floatToIntBits(object), ByteOrder.nativeOrder());
  }

  @Override
  public Float deserializeNativeObject(byte[] stream, int startPosition) {
    return Float.intBitsToFloat(CONVERTER.getInt(stream, startPosition, ByteOrder.nativeOrder()));
  }

  @Override
  public void serializeInDirectMemoryObject(Float object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    pointer.setInt(offset, Float.floatToIntBits(object));
  }

  @Override
  public Float deserializeFromDirectMemoryObject(ODirectMemoryPointer pointer, long offset) {
    return Float.intBitsToFloat(pointer.getInt(offset));
  }

  @Override
  public Float deserializeFromDirectMemoryObject(OWALChangesTree.PointerWrapper wrapper, long offset) {
    return Float.intBitsToFloat(wrapper.getInt(offset));
  }

  public void serializeNative(final float object, final byte[] stream, final int startPosition, final Object... hints) {
    CONVERTER.putInt(stream, startPosition, Float.floatToIntBits(object), ByteOrder.nativeOrder());
  }

  public float deserializeNative(final byte[] stream, final int startPosition) {
    return Float.intBitsToFloat(CONVERTER.getInt(stream, startPosition, ByteOrder.nativeOrder()));
  }

  public void serializeInDirectMemory(final float object, final ODirectMemoryPointer pointer, final long offset,
      final Object... hints) {
    pointer.setInt(offset, Float.floatToIntBits(object));
  }

  public float deserializeFromDirectMemory(final ODirectMemoryPointer pointer, final long offset) {
    return Float.intBitsToFloat(pointer.getInt(offset));
  }

  public float deserializeFromDirectMemory(OWALChangesTree.PointerWrapper wrapper, final long offset) {
    return Float.intBitsToFloat(wrapper.getInt(offset));
  }

  @Override
  public int getObjectSizeInDirectMemory(final ODirectMemoryPointer pointer, final long offset) {
    return FLOAT_SIZE;
  }

  @Override
  public int getObjectSizeInDirectMemory(OWALChangesTree.PointerWrapper wrapper, long offset) {
    return FLOAT_SIZE;
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return FLOAT_SIZE;
  }

  @Override
  public Float preprocess(final Float value, final Object... hints) {
    return value;
  }
}

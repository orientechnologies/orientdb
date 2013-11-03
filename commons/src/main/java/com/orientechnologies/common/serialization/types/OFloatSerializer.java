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

import java.nio.ByteOrder;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.OBinaryConverter;
import com.orientechnologies.common.serialization.OBinaryConverterFactory;

/**
 * Serializer for {@link Float} type.
 * 
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OFloatSerializer implements OBinarySerializer<Float> {
  private static final OBinaryConverter CONVERTER  = OBinaryConverterFactory.getConverter();

  public static OFloatSerializer        INSTANCE   = new OFloatSerializer();
  public static final byte              ID         = 7;

  /**
   * size of float value in bytes
   */
  public static final int               FLOAT_SIZE = 4;

  public int getObjectSize(Float object, Object... hints) {
    return FLOAT_SIZE;
  }

  public void serialize(Float object, byte[] stream, int startPosition, Object... hints) {
    OIntegerSerializer.INSTANCE.serialize(Float.floatToIntBits(object), stream, startPosition);
  }

  public Float deserialize(byte[] stream, int startPosition) {
    return Float.intBitsToFloat(OIntegerSerializer.INSTANCE.deserialize(stream, startPosition));
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return FLOAT_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return FLOAT_SIZE;
  }

  public void serializeNative(Float object, byte[] stream, int startPosition, Object... hints) {
    CONVERTER.putInt(stream, startPosition, Float.floatToIntBits(object), ByteOrder.nativeOrder());
  }

  public Float deserializeNative(byte[] stream, int startPosition) {
    return Float.intBitsToFloat(CONVERTER.getInt(stream, startPosition, ByteOrder.nativeOrder()));
  }

  @Override
  public void serializeInDirectMemory(Float object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    pointer.setInt(offset, Float.floatToIntBits(object));
  }

  @Override
  public Float deserializeFromDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return Float.intBitsToFloat(pointer.getInt(offset));
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return FLOAT_SIZE;
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return FLOAT_SIZE;
  }

  @Override
  public Float prepocess(Float value, Object... hints) {
    return value;
  }
}

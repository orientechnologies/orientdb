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
 * Serializer for {@link Double}
 * 
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 17.01.12
 */
public class ODoubleSerializer implements OBinarySerializer<Double> {
  private static final OBinaryConverter CONVERTER   = OBinaryConverterFactory.getConverter();

  public static ODoubleSerializer       INSTANCE    = new ODoubleSerializer();
  public static final byte              ID          = 6;

  /**
   * size of double value in bytes
   */
  public static final int               DOUBLE_SIZE = 8;

  public int getObjectSize(Double object, Object... hints) {
    return DOUBLE_SIZE;
  }

  public void serialize(Double object, byte[] stream, int startPosition, Object... hints) {
    OLongSerializer.INSTANCE.serialize(Double.doubleToLongBits(object), stream, startPosition);
  }

  public Double deserialize(byte[] stream, int startPosition) {
    return Double.longBitsToDouble(OLongSerializer.INSTANCE.deserialize(stream, startPosition));
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return DOUBLE_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return DOUBLE_SIZE;
  }

  public void serializeNative(Double object, byte[] stream, int startPosition, Object... hints) {
    CONVERTER.putLong(stream, startPosition, Double.doubleToLongBits(object), ByteOrder.nativeOrder());
  }

  public Double deserializeNative(byte[] stream, int startPosition) {
    return Double.longBitsToDouble(CONVERTER.getLong(stream, startPosition, ByteOrder.nativeOrder()));
  }

  @Override
  public void serializeInDirectMemory(Double object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    pointer.setLong(offset, Double.doubleToLongBits(object));
  }

  @Override
  public Double deserializeFromDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return Double.longBitsToDouble(pointer.getLong(offset));
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    return DOUBLE_SIZE;
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return DOUBLE_SIZE;
  }

  @Override
  public Double prepocess(Double value, Object... hints) {
    return value;
  }
}

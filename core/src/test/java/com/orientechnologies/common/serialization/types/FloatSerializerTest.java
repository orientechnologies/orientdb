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

import com.orientechnologies.orient.core.sql.parser.OCluster;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 18.01.12
 */
@Test
public class FloatSerializerTest {
  private static final int   FIELD_SIZE = 4;
  private static final Float OBJECT     = 3.14f;
  private OFloatSerializer floatSerializer;
  byte[] stream = new byte[FIELD_SIZE];

  @BeforeClass
  public void beforeClass() {
    floatSerializer = new OFloatSerializer();
  }

  public void testFieldSize() {
    Assert.assertEquals(floatSerializer.getObjectSize(null), FIELD_SIZE);
  }

  public void testSerialize() {
    floatSerializer.serialize(OBJECT, stream, 0);
    Assert.assertEquals(floatSerializer.deserialize(stream, 0), OBJECT);
  }

  public void testSerializeNative() {
    floatSerializer.serializeNative(OBJECT, stream, 0);
    Assert.assertEquals(floatSerializer.deserializeNative(stream, 0), OBJECT);
  }

  public void testNativeDirectMemoryCompatibility() {
    floatSerializer.serializeNative(OBJECT, stream, 0);

    ByteBuffer buffer = ByteBuffer.allocateDirect(stream.length).order(ByteOrder.nativeOrder());
    buffer.put(stream);
    buffer.position(0);

    Assert.assertEquals(floatSerializer.deserializeFromByteBufferObject(buffer), OBJECT);
  }

  public void testSerializeInByteBuffer() {
    final int serializationOffset = 5;

    ByteBuffer buffer = ByteBuffer.allocate(FIELD_SIZE + serializationOffset);
    buffer.position(serializationOffset);

    floatSerializer.serializeInByteBufferObject(OBJECT, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, FIELD_SIZE);

    buffer.position(serializationOffset);
    Assert.assertEquals(floatSerializer.getObjectSizeInByteBuffer(buffer), FIELD_SIZE);

    buffer.position(serializationOffset);
    Assert.assertEquals(floatSerializer.deserializeFromByteBufferObject(buffer), OBJECT);

    Assert.assertEquals(buffer.position() - serializationOffset, FIELD_SIZE);
  }
}

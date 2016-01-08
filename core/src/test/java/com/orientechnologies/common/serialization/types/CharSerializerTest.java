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

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

/**
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 19.01.12
 */
@Test
public class CharSerializerTest {
  private static final int       FIELD_SIZE = 2;
  private static final Character OBJECT     = (char) (new Random()).nextInt();
  private OCharSerializer charSerializer;
  byte[] stream = new byte[FIELD_SIZE];

  @BeforeClass
  public void beforeClass() {
    charSerializer = new OCharSerializer();
  }

  public void testFieldSize() {
    Assert.assertEquals(charSerializer.getObjectSize(null), FIELD_SIZE);
  }

  public void testSerialize() {
    charSerializer.serialize(OBJECT, stream, 0);
    Assert.assertEquals(charSerializer.deserialize(stream, 0), OBJECT);
  }

  public void testSerializeNative() {
    charSerializer.serializeNative(OBJECT, stream, 0);
    Assert.assertEquals(charSerializer.deserializeNativeObject(stream, 0), OBJECT);
  }

  public void testNativeDirectMemoryCompatibility() {
    charSerializer.serializeNative(OBJECT, stream, 0);

    ByteBuffer buffer = ByteBuffer.allocateDirect(stream.length).order(ByteOrder.nativeOrder());
    buffer.position(0);
    buffer.put(stream);
    buffer.position(0);

    Assert.assertEquals(charSerializer.deserializeFromByteBufferObject(buffer), OBJECT);
  }
}

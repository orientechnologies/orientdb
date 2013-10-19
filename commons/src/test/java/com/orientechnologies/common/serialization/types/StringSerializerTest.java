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

import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 19.01.12
 */
@Test
public class StringSerializerTest {
  private int               FIELD_SIZE;
  private String            OBJECT;
  private OStringSerializer stringSerializer;
  byte[]                    stream;

  @BeforeClass
  public void beforeClass() {
    stringSerializer = new OStringSerializer();
    Random random = new Random();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      sb.append((char) random.nextInt());
    }
    OBJECT = sb.toString();
    FIELD_SIZE = OBJECT.length() * 2 + 4;
    stream = new byte[FIELD_SIZE];
  }

  public void testFieldSize() {
    Assert.assertEquals(stringSerializer.getObjectSize(OBJECT), FIELD_SIZE);
  }

  public void testSerialize() {
    stringSerializer.serialize(OBJECT, stream, 0);
    Assert.assertEquals(stringSerializer.deserialize(stream, 0), OBJECT);
  }

  public void testSerializeNative() {
    stringSerializer.serializeNative(OBJECT, stream, 0);
    Assert.assertEquals(stringSerializer.deserializeNative(stream, 0), OBJECT);
  }

  public void testNativeDirectMemoryCompatibility() {
    stringSerializer.serializeNative(OBJECT, stream, 0);

    ODirectMemoryPointer pointer = new ODirectMemoryPointer(stream);
    try {
      Assert.assertEquals(stringSerializer.deserializeFromDirectMemory(pointer, 0), OBJECT);
    } finally {
      pointer.free();
    }
  }
}

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
import com.orientechnologies.common.directmemory.ODirectMemoryPointerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Random;

/**
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 19.01.12
 */
@Test
public class StringSerializerTest {
  byte[]                    stream;
  private int               FIELD_SIZE;
  private String            OBJECT;
  private OStringSerializer stringSerializer;

  @BeforeClass
  public void beforeClass() {
    stringSerializer = new OStringSerializer();
    Random random = new Random();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < random.nextInt(20) + 5; i++) {
      sb.append((char) random.nextInt());
    }
    OBJECT = sb.toString();
    FIELD_SIZE = OBJECT.length() * 2 + 4 + 7;
    stream = new byte[FIELD_SIZE];
  }

  public void testFieldSize() {
    Assert.assertEquals(stringSerializer.getObjectSize(OBJECT), FIELD_SIZE - 7);
  }

  public void testSerialize() {
    stringSerializer.serialize(OBJECT, stream, 7);
    Assert.assertEquals(stringSerializer.deserialize(stream, 7), OBJECT);
  }

  public void testSerializeNative() {
    stringSerializer.serializeNativeObject(OBJECT, stream, 7);
    Assert.assertEquals(stringSerializer.deserializeNativeObject(stream, 7), OBJECT);
  }

  public void testNativeDirectMemoryCompatibility() {
    stringSerializer.serializeNativeObject(OBJECT, stream, 7);

    ODirectMemoryPointer pointer = ODirectMemoryPointerFactory.instance().createPointer(stream);
    try {
      Assert.assertEquals(stringSerializer.deserializeFromDirectMemoryObject(pointer, 7), OBJECT);
    } finally {
      pointer.free();
    }
  }
}

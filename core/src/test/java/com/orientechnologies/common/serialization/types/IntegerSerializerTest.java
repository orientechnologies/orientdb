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

/**
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 17.01.12
 */
@Test
public class IntegerSerializerTest {
  private static final int     FIELD_SIZE = 4;
  private static final Integer OBJECT     = 1;
  private OIntegerSerializer   integerSerializer;
  byte[]                       stream     = new byte[FIELD_SIZE];

  @BeforeClass
  public void beforeClass() {
    integerSerializer = new OIntegerSerializer();
  }

  public void testFieldSize() {
    Assert.assertEquals(integerSerializer.getObjectSize(null), FIELD_SIZE);
  }

  public void testSerialize() {
    integerSerializer.serialize(OBJECT, stream, 0);
    Assert.assertEquals(integerSerializer.deserialize(stream, 0), OBJECT);
  }

  public void testSerializeNative() {
    integerSerializer.serializeNative(OBJECT, stream, 0);
    Assert.assertEquals(integerSerializer.deserializeNativeObject(stream, 0), OBJECT);
  }

  public void testNativeDirectMemoryCompatibility() {
    integerSerializer.serializeNative(OBJECT, stream, 0);

    ODirectMemoryPointer pointer = ODirectMemoryPointerFactory.instance().createPointer(stream);
    try {
      Assert.assertEquals(integerSerializer.deserializeFromDirectMemoryObject(pointer, 0), OBJECT);
    } finally {
      pointer.free();
    }
  }
}

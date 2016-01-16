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
 * @since 18.01.12
 */
@Test
public class BooleanSerializerTest {
  private static final int     FIELD_SIZE   = 1;
  byte[]                       stream       = new byte[FIELD_SIZE];
  private static final Boolean OBJECT_TRUE  = true;
  private static final Boolean OBJECT_FALSE = false;
  private OBooleanSerializer   booleanSerializer;

  @BeforeClass
  public void beforeClass() {
    booleanSerializer = new OBooleanSerializer();
  }

  public void testFieldSize() {
    Assert.assertEquals(booleanSerializer.getObjectSize(null), FIELD_SIZE);
  }

  public void testSerialize() {
    booleanSerializer.serialize(OBJECT_TRUE, stream, 0);
    Assert.assertEquals(booleanSerializer.deserialize(stream, 0), OBJECT_TRUE);
    booleanSerializer.serialize(OBJECT_FALSE, stream, 0);
    Assert.assertEquals(booleanSerializer.deserialize(stream, 0), OBJECT_FALSE);
  }

  public void testSerializeNative() {
    booleanSerializer.serializeNative(OBJECT_TRUE, stream, 0);
    Assert.assertEquals(booleanSerializer.deserializeNativeObject(stream, 0), OBJECT_TRUE);
    booleanSerializer.serializeNative(OBJECT_FALSE, stream, 0);
    Assert.assertEquals(booleanSerializer.deserializeNativeObject(stream, 0), OBJECT_FALSE);
  }

  public void testNativeDirectMemoryCompatibility() {
    booleanSerializer.serializeNative(OBJECT_TRUE, stream, 0);

    ODirectMemoryPointer pointer = ODirectMemoryPointerFactory.instance().createPointer(stream);
    try {
      Assert.assertEquals(booleanSerializer.deserializeFromDirectMemoryObject(pointer, 0), OBJECT_TRUE);
    } finally {
      pointer.free();
    }

    booleanSerializer.serializeNative(OBJECT_FALSE, stream, 0);
    pointer = ODirectMemoryPointerFactory.instance().createPointer(stream);
    try {
      Assert.assertEquals(booleanSerializer.deserializeFromDirectMemoryObject(pointer, 0), OBJECT_FALSE);
    } finally {
      pointer.free();
    }
  }
}

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

import java.util.Date;

/**
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 20.01.12
 */
@Test
public class DateTimeSerializerTest {
  private final static int    FIELD_SIZE = 8;
  private static final Date   OBJECT     = new Date();
  private ODateTimeSerializer dateTimeSerializer;
  private static final byte[] stream     = new byte[FIELD_SIZE];

  @BeforeClass
  public void beforeClass() {
    dateTimeSerializer = new ODateTimeSerializer();
  }

  public void testFieldSize() {
    Assert.assertEquals(dateTimeSerializer.getObjectSize(OBJECT), FIELD_SIZE);
  }

  public void testSerialize() {
    dateTimeSerializer.serialize(OBJECT, stream, 0);
    Assert.assertEquals(dateTimeSerializer.deserialize(stream, 0), OBJECT);
  }

  public void testSerializeNative() {
    dateTimeSerializer.serializeNativeObject(OBJECT, stream, 0);
    Assert.assertEquals(dateTimeSerializer.deserializeNativeObject(stream, 0), OBJECT);
  }

  public void testNativeDirectMemoryCompatibility() {
    dateTimeSerializer.serializeNativeObject(OBJECT, stream, 0);

    ODirectMemoryPointer pointer = ODirectMemoryPointerFactory.instance().createPointer(stream);
    try {
      Assert.assertEquals(dateTimeSerializer.deserializeFromDirectMemoryObject(pointer, 0), OBJECT);
    } finally {
      pointer.free();
    }

  }
}

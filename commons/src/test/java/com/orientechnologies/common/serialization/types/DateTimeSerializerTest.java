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

import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
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
    dateTimeSerializer.serializeNative(OBJECT, stream, 0);
    Assert.assertEquals(dateTimeSerializer.deserializeNative(stream, 0), OBJECT);
  }

  public void testNativeDirectMemoryCompatibility() {
    ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

    dateTimeSerializer.serializeNative(OBJECT, stream, 0);

    long pointer = directMemory.allocate(stream);
    try {
      Assert.assertEquals(dateTimeSerializer.deserializeFromDirectMemory(directMemory, pointer), OBJECT);
    } finally {
      directMemory.free(pointer);
    }

  }
}

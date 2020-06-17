/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
package com.orientechnologies.orient.test.database.speed;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadTest;
import org.testng.annotations.Test;

@Test
public class IntgerSerializationSpeedTest extends OrientMonoThreadTest {

  @Test(enabled = false)
  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    IntgerSerializationSpeedTest test = new IntgerSerializationSpeedTest();
    test.data.go(test);
  }

  public IntgerSerializationSpeedTest() throws InstantiationException, IllegalAccessException {
    super(1000000000);
  }

  @Override
  @Test(enabled = false)
  public void init() {}

  @Override
  @Test(enabled = false)
  public void cycle() {
    BytesContainer container = new BytesContainer();

    OIntegerSerializer.INSTANCE.serialize(20, container.bytes, container.offset);
    container.skip(OIntegerSerializer.INT_SIZE);
    OIntegerSerializer.INSTANCE.serialize(200, container.bytes, container.offset);
    container.skip(OIntegerSerializer.INT_SIZE);
    OIntegerSerializer.INSTANCE.serialize(20000, container.bytes, container.offset);
    container.skip(OIntegerSerializer.INT_SIZE);
    OLongSerializer.INSTANCE.serialize(200000000000000000L, container.bytes, container.offset);
    container.skip(OLongSerializer.LONG_SIZE);
    container.offset = 0;
    OIntegerSerializer.INSTANCE.deserialize(container.bytes, container.offset);
    container.skip(OIntegerSerializer.INT_SIZE);
    OIntegerSerializer.INSTANCE.deserialize(container.bytes, container.offset);
    container.skip(OIntegerSerializer.INT_SIZE);
    OIntegerSerializer.INSTANCE.deserialize(container.bytes, container.offset);
    container.skip(OIntegerSerializer.INT_SIZE);
    OLongSerializer.INSTANCE.deserialize(container.bytes, container.offset);
    container.skip(OLongSerializer.LONG_SIZE);
  }

  @Override
  @Test(enabled = false)
  public void deinit() {}
}

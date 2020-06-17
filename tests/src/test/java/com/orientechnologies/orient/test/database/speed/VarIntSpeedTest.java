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

import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSerializer;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadTest;
import org.testng.annotations.Test;

@Test
public class VarIntSpeedTest extends OrientMonoThreadTest {

  @Test(enabled = false)
  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    VarIntSpeedTest test = new VarIntSpeedTest();
    test.data.go(test);
  }

  public VarIntSpeedTest() throws InstantiationException, IllegalAccessException {
    super(1000000000);
  }

  @Override
  @Test(enabled = false)
  public void init() {}

  @Override
  @Test(enabled = false)
  public void cycle() {
    BytesContainer container = new BytesContainer();
    OVarIntSerializer.write(container, 20);
    OVarIntSerializer.write(container, 200);
    OVarIntSerializer.write(container, 20000);
    OVarIntSerializer.write(container, 200000000000000000L);
    container.offset = 0;
    OVarIntSerializer.readAsInteger(container);
    OVarIntSerializer.readAsInteger(container);
    OVarIntSerializer.readAsInteger(container);
    OVarIntSerializer.readAsLong(container);
  }

  @Override
  @Test(enabled = false)
  public void deinit() {}
}

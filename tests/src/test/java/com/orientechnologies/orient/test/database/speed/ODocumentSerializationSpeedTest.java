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

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadTest;
import org.testng.annotations.Test;

@Test(enabled = false)
public class ODocumentSerializationSpeedTest extends OrientMonoThreadTest {
  private ODocument record;

  public ODocumentSerializationSpeedTest() throws InstantiationException, IllegalAccessException {
    super(1000000);

    record = new ODocument();
  }

  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    ODocumentSerializationSpeedTest test = new ODocumentSerializationSpeedTest();
    test.data.go(test);
  }

  @Override
  public void cycle() {
    record.reset();
    record.field("id", data.getCyclesDone());
    for (int i = 0; i < 15; ++i) record.field("name" + i, "Luca" + i);
    // record.field("surname", "Garulli");
    // record.field("salary", 3000f);
    // record.field("double", 3343434d);
    // record.field("int", 23323);
    // record.field("bd", new BigDecimal("12232232.232"));
    // record.field("boolean", true);
    // record.field("bytes", new byte[] { 32, 32, 22, 2, 32, 3, 23, 2, 32 });
    final byte[] buffer = record.toStream();

    record.reset();
    record.fromStream(buffer);
    record.toString();
  }

  @Override
  public void deinit() {
    super.deinit();
  }
}

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
package com.orientechnologies.orient.test.database.speed;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadTest;

@Test(enabled = false)
public class ODocumentSerializationSpeedTest extends OrientMonoThreadTest {
  private ODocument record;

  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    ODocumentSerializationSpeedTest test = new ODocumentSerializationSpeedTest();
    test.data.go(test);
  }

  public ODocumentSerializationSpeedTest() throws InstantiationException, IllegalAccessException {
    super(1000000);

    record = new ODocument();
  }

  @Override
  public void cycle() {
    record.reset();
    record.field("id", data.getCyclesDone());
    record.field("name", "Luca");
    record.field("surname", "Garulli");
    record.field("salary", 3000f);
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

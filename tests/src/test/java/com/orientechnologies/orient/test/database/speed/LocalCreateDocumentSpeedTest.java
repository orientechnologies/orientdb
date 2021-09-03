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

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadTest;
import java.util.Date;
import org.testng.annotations.Test;

@Test
public class LocalCreateDocumentSpeedTest extends OrientMonoThreadTest {
  private ODatabaseDocumentTx database;
  private ODocument record;
  private Date date = new Date();

  public LocalCreateDocumentSpeedTest() throws InstantiationException, IllegalAccessException {
    super(1000000);
  }

  @Test(enabled = false)
  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    LocalCreateDocumentSpeedTest test = new LocalCreateDocumentSpeedTest();
    test.data.go(test);
  }

  @Override
  @Test(enabled = false)
  public void init() {
    database = new ODatabaseDocumentTx(System.getProperty("url"));
    database.setSerializer(new ORecordSerializerBinary());

    if (database.exists()) {
      database.open("admin", "admin");
      database.drop();
    }

    database.create();
    OSchema schema = database.getMetadata().getSchema();
    schema.createClass("Account");

    record = database.newInstance();

    database.declareIntent(new OIntentMassiveInsert());
    database.begin(TXTYPE.NOTX);
  }

  @Override
  @Test(enabled = false)
  public void cycle() {
    record.reset();

    record.setClassName("Account");
    record.field("id", data.getCyclesDone());
    record.field("name", "Luca");
    record.field("surname", "Garulli");
    record.field("birthDate", date);
    record.field("salary", 3000f + data.getCyclesDone());

    record.save();

    if (data.getCyclesDone() == data.getCycles() - 1) database.commit();
  }

  @Override
  @Test(enabled = false)
  public void deinit() {
    System.out.println(Orient.instance().getProfiler().dump());

    if (database != null) database.close();
    super.deinit();
  }
}

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

import com.orientechnologies.common.test.SpeedTestMultiThreads;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.test.database.base.OrientMultiThreadTest;
import com.orientechnologies.orient.test.database.base.OrientThreadTest;
import java.util.Date;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LocalCreateDocumentMultiThreadSpeedTest extends OrientMultiThreadTest {
  private ODatabaseDocumentTx mainDatabase;
  private long foundObjects;

  @Test(enabled = false)
  public static class CreateObjectsThread extends OrientThreadTest {
    private ODatabaseDocumentTx database;
    private ODocument record;
    private Date date = new Date();

    public CreateObjectsThread(final SpeedTestMultiThreads parent, final int threadId) {
      super(parent, threadId);
    }

    @Override
    public void init() {
      database = new ODatabaseDocumentTx(System.getProperty("url")).open("admin", "admin");
      database.setSerializer(new ORecordSerializerBinary());

      record = database.newInstance();
      database.declareIntent(new OIntentMassiveInsert());
      database.begin(TXTYPE.NOTX);
    }

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
    public void deinit() throws Exception {
      if (database != null) database.close();
      super.deinit();
    }
  }

  public LocalCreateDocumentMultiThreadSpeedTest() {
    super(1000000, 8, CreateObjectsThread.class);
  }

  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    // System.setProperty("url", "memory:test");
    OGlobalConfiguration.WAL_SYNC_ON_PAGE_FLUSH.setValue(false);
    LocalCreateDocumentMultiThreadSpeedTest test = new LocalCreateDocumentMultiThreadSpeedTest();
    test.data.go(test);
    OGlobalConfiguration.WAL_SYNC_ON_PAGE_FLUSH.setValue(true);
  }

  @Override
  public void init() {
    mainDatabase = new ODatabaseDocumentTx(System.getProperty("url"));
    mainDatabase.setSerializer(new ORecordSerializerBinary());
    if (mainDatabase.exists()) {
      mainDatabase.open("admin", "admin");
      // else
      mainDatabase.drop();
    }

    mainDatabase.create();
    mainDatabase.set(ODatabase.ATTRIBUTES.MINIMUMCLUSTERS, 8);
    mainDatabase.getMetadata().getSchema().createClass("Account");

    foundObjects = 0; // database.countClusterElements("Account");

    System.out.println("\nTotal objects in Animal cluster before the test: " + foundObjects);
  }

  @Override
  public void deinit() {
    Assert.assertEquals(mainDatabase.countClass("Account"), 1000000 + foundObjects);

    if (mainDatabase != null) mainDatabase.close();
  }
}

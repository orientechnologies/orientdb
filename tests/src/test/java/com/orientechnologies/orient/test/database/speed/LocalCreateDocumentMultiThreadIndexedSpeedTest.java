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

import com.orientechnologies.common.test.SpeedTestMultiThreads;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.test.database.base.OrientMultiThreadTest;
import com.orientechnologies.orient.test.database.base.OrientThreadTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Date;


public class LocalCreateDocumentMultiThreadIndexedSpeedTest extends OrientMultiThreadTest {
  private ODatabaseDocument database;
  private long              foundObjects;

  @Test(enabled = false)
  public static class CreateObjectsThread extends OrientThreadTest {
    private ODatabaseDocument database;
    private ODocument         record;
    private Date              date = new Date();

    public CreateObjectsThread(final SpeedTestMultiThreads parent, final int threadId) {
      super(parent, threadId);
    }

    @Override
    public void init() {
      database = new ODatabaseDocumentTx(System.getProperty("url")).open("admin", "admin");
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

      if (data.getCyclesDone() == data.getCycles() - 1)
        database.commit();
    }

    @Override
    public void deinit() throws Exception {
      if (database != null)
        database.close();
      super.deinit();
    }
  }

  public LocalCreateDocumentMultiThreadIndexedSpeedTest(int tot, int threads) {
    super(tot, threads, CreateObjectsThread.class);

    Orient.instance().getProfiler().startRecording();
  }

  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    // System.setProperty("url", "remote:localhost/demo");

    if (iArgs.length > 0)
      System.setProperty("url", iArgs[0]);

    int tot = iArgs.length > 1 ? Integer.parseInt(iArgs[1]) : 1000000;
    int threads = iArgs.length > 2 ? Integer.parseInt(iArgs[2]) : 5;

    LocalCreateDocumentMultiThreadIndexedSpeedTest test = new LocalCreateDocumentMultiThreadIndexedSpeedTest(tot, threads);
    test.data.go(test);
  }

  @Override
  public void init() {
    database = new ODatabaseDocumentTx(System.getProperty("url"));
    database.setProperty("minPool", 2);
    database.setProperty("maxPool", 3);

    if (database.getURL().startsWith("remote:"))
      database.open("admin", "admin");
    else {
      if (database.exists())
        database.drop();

      database.create();
    }

    foundObjects = 0;// database.countClusterElements("Account");

    synchronized (LocalCreateDocumentMultiThreadIndexedSpeedTest.class) {
      // database.command(new OCommandSQL("truncate class account")).execute();

      OClass c = database.getMetadata().getSchema().getClass("Account");
      if (c == null)
        c = database.getMetadata().getSchema().createClass("Account");

      OProperty p = database.getMetadata().getSchema().getClass("Account").getProperty("id");
      if (p == null)
        p = database.getMetadata().getSchema().getClass("Account").createProperty("id", OType.INTEGER);

      if (!p.isIndexed())
        p.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    }

    System.out.println("\nTotal objects in Animal cluster before the test: " + foundObjects);
  }

  @Override
  public void deinit() {
//    long total = database.countClusterElements("Account");
//
//    System.out.println("\nTotal objects in Account cluster after the test: " + total);
//    System.out.println("Created " + (total - foundObjects));
//    Assert.assertEquals(total - foundObjects, threadCycles);

    if (database != null)
      database.close();

    System.out.println(Orient.instance().getProfiler().dump());

  }
}

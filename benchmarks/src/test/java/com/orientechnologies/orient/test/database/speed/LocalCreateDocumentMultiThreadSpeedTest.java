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
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.test.database.base.OrientMultiThreadDBTest;
import com.orientechnologies.orient.test.database.base.OrientThreadTest;
import java.util.Date;
import org.junit.Assert;

public class LocalCreateDocumentMultiThreadSpeedTest extends OrientMultiThreadDBTest {
  private ODatabaseSession mainDatabase;
  private long foundObjects;

  public static class CreateObjectsThread extends OrientThreadTest {
    private ODatabaseDocument database;
    private ODocument record;
    private Date date = new Date();

    public CreateObjectsThread(final SpeedTestMultiThreads parent, final int threadId) {
      super(parent, threadId);
    }

    @Override
    public void init() {
      database = ((OrientMultiThreadDBTest) owner).openDB();

      record = database.newInstance();
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

      database.save(record);

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
    LocalCreateDocumentMultiThreadSpeedTest test = new LocalCreateDocumentMultiThreadSpeedTest();
    test.data.go(test);
  }

  @Override
  public void init() throws Exception {
    super.init();
    dropAndCreate();
    mainDatabase = openDB();
    mainDatabase.set(ODatabase.ATTRIBUTES.MINIMUMCLUSTERS, 8);
    mainDatabase.getMetadata().getSchema().createClass("Account");

    foundObjects = 0; // database.countClusterElements("Account");

    System.out.println("\nTotal objects in Animal cluster before the test: " + foundObjects);
  }

  @Override
  public void deinit() throws Exception {
    Assert.assertEquals(mainDatabase.countClass("Account"), 1000000 + foundObjects);

    if (mainDatabase != null) mainDatabase.close();
    super.deinit();
  }
}

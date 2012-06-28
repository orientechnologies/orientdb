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

import java.util.Date;

import org.testng.annotations.Test;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.db.ODatabaseComplex.OPERATION_MODE;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadTest;

@Test(enabled = false)
public class RemoteCreateDocumentSpeedTest extends OrientMonoThreadTest {
  private ODatabaseDocument database;
  private ODocument         record;
  private Date              date  = new Date();
  private final static long DELAY = 0;

  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    RemoteCreateDocumentSpeedTest test = new RemoteCreateDocumentSpeedTest();
    test.data.go(test);
  }

  public RemoteCreateDocumentSpeedTest() throws InstantiationException, IllegalAccessException {
    super(1000000);
  }

  @Override
  public void init() {
    OProfiler.getInstance().startRecording();

    database = new ODatabaseDocumentTx(System.getProperty("url")).open("admin", "admin");
    record = database.newInstance();

    database.declareIntent(new OIntentMassiveInsert());
    database.begin(TXTYPE.NOTX);
  }

  @Override
  public void cycle() {
    record.reset();

    record.setClassName("Account");
    record.field("id", data.getCyclesDone());
    record.field("name", "Luca");
    record.field("surname", "Garulli");
    record.field("birthDate", date);
    record.field("salary", 3000f + data.getCyclesDone());

    database.save(record, OPERATION_MODE.ASYNCHRONOUS_NOANSWER, null);

    if (data.getCyclesDone() == data.getCycles() - 1)
      database.commit();

    if (DELAY > 0)
      try {
        Thread.sleep(DELAY);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
  }

  @Override
  public void deinit() {
    System.out.println(OProfiler.getInstance().dump());

    if (database != null)
      database.close();
    super.deinit();
  }
}

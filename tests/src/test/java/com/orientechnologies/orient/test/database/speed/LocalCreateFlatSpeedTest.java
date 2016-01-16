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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadTest;

@Test(enabled = false)
public class LocalCreateFlatSpeedTest extends OrientMonoThreadTest {
  private ODatabaseDocumentTx database;
  private ORecordFlat   record;
  private long          date = System.currentTimeMillis();

  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    OGlobalConfiguration.USE_WAL.setValue(false);
    LocalCreateFlatSpeedTest test = new LocalCreateFlatSpeedTest();
    test.data.go(test);
    OGlobalConfiguration.USE_WAL.setValue(true);
  }

  public LocalCreateFlatSpeedTest() throws InstantiationException, IllegalAccessException {
    super(1000000);
  }

  @Override
  public void init() {
    Orient.instance().getProfiler().startRecording();

    database = new ODatabaseDocumentTx(System.getProperty("url"));
    if (database.exists())
      database.open("admin", "admin");
    else
      database.create();

    record = new ORecordFlat();

    database.declareIntent(new OIntentMassiveInsert());
    database.begin(TXTYPE.NOTX);
  }

  @Override
  public void cycle() {
    record.reset();
    record.value(
        "Account@id:" + data.getCyclesDone() + ",name:'Luca',surname:'Garulli',birthDate:" + date + "salary:"
            + (data.getCyclesDone() + 3000) + ".00").save("flat");

    if (data.getCyclesDone() == data.getCycles() - 1)
      database.commit();
  }

  @Override
  public void deinit() {
    if (database != null)
      database.close();
    super.deinit();
  }
}

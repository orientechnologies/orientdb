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
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.test.database.base.OrientMonoThreadTest;

@Test(enabled = false)
public class TxRemoteCreateFlatSpeedTest extends OrientMonoThreadTest {
  private ODatabaseDocumentTx database;
  private ORecordFlat   record;

  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    TxRemoteCreateFlatSpeedTest test = new TxRemoteCreateFlatSpeedTest();
    test.data.go(test);
  }

  public TxRemoteCreateFlatSpeedTest() throws InstantiationException, IllegalAccessException {
    super(1000000);
  }

  @Override
  public void init() {
    Orient.instance().getProfiler().startRecording();

    database = new ODatabaseDocumentTx(System.getProperty("url")).open("admin", "admin");
    record = new ORecordFlat();

    database.begin();
  }

  @Override
  public void cycle() {
    record.value(
        "{id:" + data.getCyclesDone() + ",name:'Gipsy',type:'Cat',race:'European',country:'Italy',price:"
            + (data.getCyclesDone() + 300) + ".00}").save("Animal");
  }

  @Override
  public void deinit() {
    database.commit();

    database.close();
    super.deinit();
  }
}

/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.record.ODatabaseFlat;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.test.database.base.OrientMultiThreadTest;
import com.orientechnologies.orient.test.database.base.OrientThreadTest;

public class TxRemoteCreateObjectsMultiThreadSpeedTest extends OrientMultiThreadTest {
  protected ODatabaseFlat database;
  protected long          foundObjects;

  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    TxRemoteCreateObjectsMultiThreadSpeedTest test = new TxRemoteCreateObjectsMultiThreadSpeedTest();
    test.data.go(test);
  }

  public TxRemoteCreateObjectsMultiThreadSpeedTest() {
    super(1000000, 10, CreateObjectsThread.class);
    Orient.instance().registerEngine(new OEngineRemote());
  }

  @Override
  public void init() {
    database = new ODatabaseFlat(System.getProperty("url")).open("admin", "admin");

    if (!database.getStorage().getClusterNames().contains("Animal"))
      database.getStorage().addCluster("Animal", OStorage.CLUSTER_TYPE.PHYSICAL);

    foundObjects = database.countClusterElements("Animal");
    System.out.println("\nTotal objects in Animal cluster before the test: " + foundObjects);
  }

  public static class CreateObjectsThread extends OrientThreadTest {
    protected ODatabaseFlat database;
    protected ORecordFlat   record = new ORecordFlat();

    @Override
    public void init() {
      database = new ODatabaseFlat(System.getProperty("url")).open("admin", "admin");
      record = database.newInstance();

      database.begin(TXTYPE.NOTX);
    }

    public void cycle() {
      record.reset();
      record.value(data.getCyclesDone() + "|Gipsy|Cat|European|Italy|" + (data.getCyclesDone() + 300) + ".00").save("csv");

      if (data.getCyclesDone() >= data.getCycles() - 1)
        database.commit();
    }

    @Override
    public void deinit() throws Exception {
      database.close();
      super.deinit();
    }
  }

  @Override
  public void deinit() {
    System.out.println("\nTotal objects in Animal cluster after the test: " + (database.countClusterElements("Animal")));

    System.out.println("Created " + (database.countClusterElements("Animal") - foundObjects));

    assert threadCycles == database.countClusterElements("Animal") - foundObjects;

    if (database != null)
      database.close();
  }
}

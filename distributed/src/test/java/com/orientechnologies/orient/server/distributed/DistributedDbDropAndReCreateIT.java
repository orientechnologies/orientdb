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
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.setup.ServerRun;
import org.junit.Assert;
import org.junit.Test;

/** Distributed test on drop + recreate database. */
public class DistributedDbDropAndReCreateIT extends AbstractServerClusterTxTest {
  private static final int SERVERS = 3;

  @Test
  public void test() throws Exception {
    count = 10;
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void onAfterExecution() throws Exception {
    int s = 0;
    do {
      ServerRun server = serverInstance.get(0);
      OrientDB orientDB = server.getServerInstance().getContext();

      banner("DROPPING DATABASE ON SERVERS");

      waitForDatabaseIsOnline(0, "europe-0", getDatabaseName(), 5000);
      waitForDatabaseIsOnline(0, "europe-1", getDatabaseName(), 5000);
      waitForDatabaseIsOnline(0, "europe-2", getDatabaseName(), 5000);

      orientDB.drop(getDatabaseName());

      Thread.sleep(2000);

      Assert.assertNull(
          server
              .getServerInstance()
              .getDistributedManager()
              .getOnlineDatabaseConfiguration(getDatabaseName()));

      server = serverInstance.get(s);

      banner("RE-CREATING DATABASE ON SERVER " + server.getServerId());

      Assert.assertNull(
          server
              .getServerInstance()
              .getDistributedManager()
              .getOnlineDatabaseConfiguration(getDatabaseName()));

      for (int retry = 0; retry < 10; retry++) {
        try {
          orientDB.execute(
              "create database ? plocal users(admin identified by 'admin' role admin)",
              getDatabaseName());
          break;
        } catch (ODatabaseException e) {
          System.out.println("DB STILL IN THE CLUSTER, WAIT AND RETRY (retry " + retry + ")...");
          Thread.sleep(1000);
        }
      }

    } while (++s < serverInstance.size());

    // DROP LAST DATABASE
    serverInstance
        .get(serverInstance.size() - 1)
        .getServerInstance()
        .dropDatabase(getDatabaseName());
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  public String getDatabaseName() {
    return "distributed-DistributedDbDropAndReCreateIT";
  }
}

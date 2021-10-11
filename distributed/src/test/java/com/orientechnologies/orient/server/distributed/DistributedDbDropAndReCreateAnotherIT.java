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

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.setup.ServerRun;
import org.junit.Test;

/** Distributed test on drop + recreate database with a different name. */
public class DistributedDbDropAndReCreateAnotherIT extends AbstractServerClusterTxTest {
  static final int SERVERS = 3;
  private int lastServerNum = 0;

  // TODO: is distributed
  @Test
  public void test() throws Exception {
    count = 10;
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void onAfterExecution() throws Exception {
    do {
      ServerRun server = serverInstance.get(0);

      waitForDatabaseIsOnline(0, "europe-0", getDatabaseName(), 5000);
      waitForDatabaseIsOnline(0, "europe-1", getDatabaseName(), 5000);
      waitForDatabaseIsOnline(0, "europe-2", getDatabaseName(), 5000);

      server.getServerInstance().dropDatabase(getDatabaseName());

      waitForDatabaseIsOffline("europe-0", getDatabaseName(), 5000);
      waitForDatabaseIsOffline("europe-1", getDatabaseName(), 5000);
      waitForDatabaseIsOffline("europe-2", getDatabaseName(), 5000);

      server = serverInstance.get(lastServerNum);

      ++lastServerNum;

      final String dbName = getDatabaseURL(server);

      banner("(RE)CREATING DATABASE " + dbName + " ON SERVER " + server.getServerId());

      server
          .getServerInstance()
          .getContext()
          .execute(
              "create database ? plocal users(admin identified by 'admin' role admin)",
              getDatabaseName());

      waitForDatabaseIsOnline(0, "europe-0", getDatabaseName(), 15000);
      waitForDatabaseIsOnline(0, "europe-1", getDatabaseName(), 15000);
      waitForDatabaseIsOnline(0, "europe-2", getDatabaseName(), 15000);

      checkSameClusters();

      try (ODatabaseDocument graph =
          server.getServerInstance().openDatabase(getDatabaseName(), "admin", "admin")) {
        onAfterDatabaseCreation(graph);
      }

      checkThePersonClassIsPresentOnAllTheServers();

    } while (lastServerNum < serverInstance.size());

    banner("EXECUTING FINAL TESTS");

    Thread.sleep(10000);

    executeMultipleTest(0);
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  public String getDatabaseName() {
    return "distributed-dropcreatedbnewname" + lastServerNum;
  }
}

/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import org.junit.Assert;
import org.junit.Test;

/**
 * Distributed test with 3 servers running (0, 1 and 2) and after a while the server 2 is isolated from the network (using a proxy)
 * and then it re-merges the cluster again.
 */
public class SplitBrainNetwork3StaticServersTest extends AbstractHARemoveNode {
  final static int SERVERS = 3;

  @Test
  public void test() throws Exception {
    useTransactions = true;
    count = 10;
    startupNodesInSequence = true;
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void onAfterExecution() throws Exception {
    Assert.assertEquals("europe-0", serverInstance.get(0).getServerInstance().getDistributedManager().getCoordinatorServer());
    Assert.assertEquals("europe-0", serverInstance.get(1).getServerInstance().getDistributedManager().getCoordinatorServer());
    Assert.assertEquals("europe-0", serverInstance.get(2).getServerInstance().getDistributedManager().getCoordinatorServer());

    banner("SIMULATE ISOLATION OF SERVER " + (SERVERS - 1) + "...");

    checkInsertedEntries();
    checkIndexedEntries();

    serverInstance.get(2).disconnectFrom(serverInstance.get(0), serverInstance.get(1));

    banner("SERVER " + (SERVERS - 1) + " HAS BEEN ISOLATED, WAITING FOR THE DATABASE ON SERVER 2 TO BE OFFLINE...");

    Assert.assertEquals("europe-0", serverInstance.get(0).getServerInstance().getDistributedManager().getCoordinatorServer());
    Assert.assertEquals("europe-0", serverInstance.get(1).getServerInstance().getDistributedManager().getCoordinatorServer());
    Assert.assertEquals("europe-2", serverInstance.get(2).getServerInstance().getDistributedManager().getCoordinatorServer());

    // CHECK THE SPLIT
    waitForDatabaseStatus(0, "europe-2", getDatabaseName(), ODistributedServerManager.DB_STATUS.NOT_AVAILABLE, 30000);
    assertDatabaseStatusEquals(0, "europe-2", getDatabaseName(), ODistributedServerManager.DB_STATUS.NOT_AVAILABLE);
    assertDatabaseStatusEquals(0, "europe-1", getDatabaseName(), ODistributedServerManager.DB_STATUS.ONLINE);

    waitForDatabaseStatus(1, "europe-2", getDatabaseName(), ODistributedServerManager.DB_STATUS.NOT_AVAILABLE, 90000);
    assertDatabaseStatusEquals(1, "europe-2", getDatabaseName(), ODistributedServerManager.DB_STATUS.NOT_AVAILABLE);
    assertDatabaseStatusEquals(1, "europe-0", getDatabaseName(), ODistributedServerManager.DB_STATUS.ONLINE);

    waitForDatabaseStatus(2, "europe-0", getDatabaseName(), ODistributedServerManager.DB_STATUS.NOT_AVAILABLE, 30000);
    assertDatabaseStatusEquals(2, "europe-0", getDatabaseName(), ODistributedServerManager.DB_STATUS.NOT_AVAILABLE);
    waitForDatabaseStatus(2, "europe-1", getDatabaseName(), ODistributedServerManager.DB_STATUS.NOT_AVAILABLE, 30000);
    assertDatabaseStatusEquals(2, "europe-1", getDatabaseName(), ODistributedServerManager.DB_STATUS.NOT_AVAILABLE);
    assertDatabaseStatusEquals(2, "europe-2", getDatabaseName(), ODistributedServerManager.DB_STATUS.ONLINE);

    banner("RUN TEST WITHOUT THE OFFLINE SERVER " + (SERVERS - 1) + "...");

    checkInsertedEntries();
    checkIndexedEntries();

    count = 10;
    final long currentRecords = expected;

    executeTestsOnServers = createServerList(0, 1);
    try {
      executeMultipleTest();
    } catch (AssertionError e) {
      final String message = e.getMessage();
      Assert.assertTrue(message,
          message.contains("count is not what was expected expected:<" + expected + "> but was:<" + (currentRecords) + ">"));
    }

    banner("TEST WITH THE ISOLATED NODE FINISHED, REJOIN THE SERVER " + (SERVERS - 1) + "...");

    // FORCE THE REJOIN
    serverInstance.get(2).rejoin(serverInstance.get(0), serverInstance.get(1));

    count = 1000;
    // CREATE NEW RECORD IN THE MEANWHILE ON SERVERS 1 AND 2
    banner("RUNNING ONE WRITER ONLY ON SERVER 0");
    createWriter(0, 100, getDatabaseURL(serverInstance.get(0))).call();
    banner("RUNNING ONE WRITER ONLY ON SERVER 1");
    createWriter(1, 101, getDatabaseURL(serverInstance.get(1))).call();

    expected += count * 2;

    count = 10;

    waitForDatabaseIsOnline(0, "europe-0", getDatabaseName(), 90000);
    waitForDatabaseIsOnline(0, "europe-1", getDatabaseName(), 5000);
    waitForDatabaseIsOnline(0, "europe-2", getDatabaseName(), 5000);

    waitForDatabaseIsOnline(1, "europe-0", getDatabaseName(), 5000);
    waitForDatabaseIsOnline(1, "europe-1", getDatabaseName(), 5000);
    waitForDatabaseIsOnline(1, "europe-2", getDatabaseName(), 5000);

    waitForDatabaseIsOnline(2, "europe-0", getDatabaseName(), 5000);
    waitForDatabaseIsOnline(2, "europe-1", getDatabaseName(), 5000);
    waitForDatabaseIsOnline(2, "europe-2", getDatabaseName(), 5000);

    assertDatabaseStatusEquals(0, "europe-2", getDatabaseName(), ODistributedServerManager.DB_STATUS.ONLINE);
    assertDatabaseStatusEquals(1, "europe-2", getDatabaseName(), ODistributedServerManager.DB_STATUS.ONLINE);
    assertDatabaseStatusEquals(2, "europe-2", getDatabaseName(), ODistributedServerManager.DB_STATUS.ONLINE);

    Assert.assertEquals("europe-0", serverInstance.get(0).getServerInstance().getDistributedManager().getCoordinatorServer());
    Assert.assertEquals("europe-0", serverInstance.get(1).getServerInstance().getDistributedManager().getCoordinatorServer());
    Assert.assertEquals("europe-0", serverInstance.get(2).getServerInstance().getDistributedManager().getCoordinatorServer());

    banner("NETWORK FOR THE ISOLATED NODE " + (SERVERS - 1) + " HAS BEEN RESTORED");

    poolFactory.reset();

    checkInsertedEntries();
    checkIndexedEntries();

    banner("RESTARTING TESTS WITH SERVER " + (SERVERS - 1) + " CONNECTED...");

    count = 10;

    executeMultipleTest();
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  protected String getDistributedServerConfiguration(final ServerRun server) {
    return "proxied-orientdb-dserver-config-" + server.getServerId() + ".xml";
  }

  @Override
  public String getDatabaseName() {
    return "ha-split-2-static";
  }
}

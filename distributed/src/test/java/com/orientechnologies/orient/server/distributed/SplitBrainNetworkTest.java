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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Distributed test with 3 servers running and after a while the server 2 is isolated from the network (using a proxy) and then it
 * re-merges the cluster again.
 */
public class SplitBrainNetworkTest extends AbstractHARemoveNode {
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
    banner("SIMULATE ISOLATION OF SERVER " + (SERVERS - 1) + "...");

    checkRecordCount();

    serverInstance.get(2).disconnectFrom(serverInstance.get(0), serverInstance.get(1));

    banner("SERVER " + (SERVERS - 1) + " HAS BEEN ISOLATED, WAITING FOR THE DATABASE ON SERVER 2 TO BE OFFLINE...");

    // CHECK THE SPLIT
    waitForDatabaseStatus(0, "europe-2", getDatabaseName(), ODistributedServerManager.DB_STATUS.NOT_AVAILABLE, 90000);
    waitForDatabaseStatus(2, "europe-0", getDatabaseName(), ODistributedServerManager.DB_STATUS.NOT_AVAILABLE, 90000);
    waitForDatabaseStatus(2, "europe-1", getDatabaseName(), ODistributedServerManager.DB_STATUS.NOT_AVAILABLE, 90000);

    assertDatabaseStatusEquals(0, "europe-2", getDatabaseName(), ODistributedServerManager.DB_STATUS.NOT_AVAILABLE);

    waitForDatabaseStatus(1, "europe-2", getDatabaseName(), ODistributedServerManager.DB_STATUS.NOT_AVAILABLE, 90000);
    assertDatabaseStatusEquals(1, "europe-2", getDatabaseName(), ODistributedServerManager.DB_STATUS.NOT_AVAILABLE);

    assertDatabaseStatusEquals(2, "europe-2", getDatabaseName(), ODistributedServerManager.DB_STATUS.ONLINE);
    assertDatabaseStatusEquals(2, "europe-1", getDatabaseName(), ODistributedServerManager.DB_STATUS.NOT_AVAILABLE);
    assertDatabaseStatusEquals(2, "europe-0", getDatabaseName(), ODistributedServerManager.DB_STATUS.NOT_AVAILABLE);

    banner("RUN TEST WITHOUT THE OFFLINE SERVER " + (SERVERS - 1) + "...");

    checkRecordCount();

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

    // dumpDistributedMap();

    // FORCE THE REJOIN
    serverInstance.get(2).rejoin(serverInstance.get(0), serverInstance.get(1));

    count = 1000;
    // CREATE NEW RECORD IN THE MEANWHILE ON SERVERS 1 AND 2
    banner("RUNNING ONE WRITER ONLY ON SERVER 0");
    createWriter(0, 100, getDatabaseURL(serverInstance.get(0))).call();
    banner("RUNNING ONE WRITER ONLY ON SERVER 1");
    createWriter(1, 101, getDatabaseURL(serverInstance.get(1))).call();

    expected += count * 2;

    Thread.sleep(5000);

    count = 10;

    // dumpDistributedMap();

    waitForDatabaseIsOnline(0, "europe-2", getDatabaseName(), 90000);
    assertDatabaseStatusEquals(0, "europe-2", getDatabaseName(), ODistributedServerManager.DB_STATUS.ONLINE);
    assertDatabaseStatusEquals(1, "europe-2", getDatabaseName(), ODistributedServerManager.DB_STATUS.ONLINE);
    assertDatabaseStatusEquals(2, "europe-2", getDatabaseName(), ODistributedServerManager.DB_STATUS.ONLINE);

    banner("NETWORK FOR THE ISOLATED NODE " + (SERVERS - 1) + " HAS BEEN RESTORED");

    checkRecordCount();

    banner("RESTARTING TESTS WITH SERVER " + (SERVERS - 1) + " CONNECTED...");

    count = 10;

    executeMultipleTest();
  }

  private void dumpDistributedMap() {
    for (ServerRun s : serverInstance) {
      OLogManager.instance().info(this, "MAP SERVER %s", s.getServerId());
      for (Map.Entry<String, Object> entry : s.server.getDistributedManager().getConfigurationMap().entrySet()) {
        OLogManager.instance().info(this, " %s=%s", entry.getKey(), entry.getValue());
      }
    }
  }

  private void checkRecordCount() {
    for (ServerRun s : serverInstance) {
      final ODatabaseDocumentTx db = new ODatabaseDocumentTx(getDatabaseURL(s)).open("admin", "admin");
      try {
        final long found = db.countClass("Person");
        Assert.assertEquals("Server " + s + " expected " + expected + " but found " + found, expected, found);
      } finally {
        db.close();
      }
    }
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
    return "distributed-split";
  }
}

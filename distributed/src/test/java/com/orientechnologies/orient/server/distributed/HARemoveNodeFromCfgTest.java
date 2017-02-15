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

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Distributed TX test against "plocal" protocol + shutdown and restart of a node.
 */
public class HARemoveNodeFromCfgTest extends AbstractServerClusterTxTest {
  final static int      SERVERS      = 3;
  private AtomicBoolean lastNodeIsUp = new AtomicBoolean(true);

  @Test
  public void test() throws Exception {
    OGlobalConfiguration.DISTRIBUTED_AUTO_REMOVE_OFFLINE_SERVERS.setValue(100);
    try {

      useTransactions = true;
      count = 10;
      init(SERVERS);
      prepare(false);
      execute();

    } finally {
      OGlobalConfiguration.DISTRIBUTED_AUTO_REMOVE_OFFLINE_SERVERS.setValue(100);
    }

  }

  @Override
  protected void onAfterExecution() throws Exception {
    final String removedServer = serverInstance.get(SERVERS - 1).getServerInstance().getDistributedManager().getLocalNodeName();

    Assert.assertTrue(serverInstance.get(0).getServerInstance().getDistributedManager().getDatabaseConfiguration(getDatabaseName())
        .getAllConfiguredServers().contains(removedServer));

    Assert.assertTrue(serverInstance.get(0).getServerInstance().getDistributedManager().getConfigurationMap()
        .containsKey("dbstatus." + removedServer + "." + getDatabaseName()));

    banner("SIMULATE SOFT SHUTDOWN OF SERVER " + (SERVERS - 1));
    serverInstance.get(SERVERS - 1).shutdownServer();

    lastNodeIsUp.set(false);

    banner("RESTARTING TESTS WITH SERVER " + (SERVERS - 1) + " DOWN...");

    count = 10;

    executeMultipleTest();

    banner("RESTARTING SERVER " + (SERVERS - 1) + "...");

    Assert.assertFalse(serverInstance.get(0).getServerInstance().getDistributedManager().getDatabaseConfiguration(getDatabaseName())
        .getAllConfiguredServers().contains(removedServer));

    Assert.assertEquals(serverInstance.get(0).getServerInstance().getDistributedManager().getConfigurationMap()
        .get("dbstatus." + removedServer + "." + getDatabaseName()), ODistributedServerManager.DB_STATUS.NOT_AVAILABLE);

    serverInstance.get(SERVERS - 1).startServer(getDistributedServerConfiguration(serverInstance.get(SERVERS - 1)));
    if (serverInstance.get(SERVERS - 1).server.getPluginByClass(OHazelcastPlugin.class) != null)
      serverInstance.get(SERVERS - 1).server.getPluginByClass(OHazelcastPlugin.class).waitUntilNodeOnline();

    lastNodeIsUp.set(true);

    banner("RESTARTING TESTS WITH SERVER " + (SERVERS - 1) + " UP...");

    count = 10;

    executeMultipleTest();
  }

  @Override
  protected void onBeforeChecks() throws InterruptedException {
    // // WAIT UNTIL THE END
    waitFor(0, new OCallable<Boolean, ODatabaseDocumentTx>() {
      @Override
      public Boolean call(ODatabaseDocumentTx db) {
        final boolean ok = db.countClass("Person") >= expected;
        if (!ok)
          System.out.println("FOUND " + db.countClass("Person") + " people instead of expected " + expected);
        return ok;
      }
    }, 10000);

    waitFor(2, new OCallable<Boolean, ODatabaseDocumentTx>() {
      @Override
      public Boolean call(ODatabaseDocumentTx db) {
        final long node2Expected = lastNodeIsUp.get() ? expected : expected - (count * writerCount * (SERVERS - 1));

        final boolean ok = db.countClass("Person") >= node2Expected;
        if (!ok)
          System.out.println("FOUND " + db.countClass("Person") + " people instead of expected " + node2Expected);
        return ok;
      }
    }, 10000);
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  public String getDatabaseName() {
    return "HARemoveNodeFromCfgTest";
  }
}

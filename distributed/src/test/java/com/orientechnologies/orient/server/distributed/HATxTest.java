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

import org.junit.Test;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

/**
 * Distributed TX test by using transactions against "plocal" protocol + shutdown and restart of a node.
 */
public class HATxTest extends AbstractServerClusterTxTest {
  final static int SERVERS = 3;

  @Test
  public void test() throws Exception {
    useTransactions = true;
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void onAfterExecution() throws Exception {
    banner("SIMULATE SOFT SHUTDOWN OF SERVER " + (SERVERS - 1));
    serverInstance.get(SERVERS - 1).shutdownServer();

    banner("RESTARTING TESTS WITH SERVER " + (SERVERS - 1) + " DOWN...");

    count = 200;

    executeMultipleTest();

    banner("RESTARTING SERVER " + (SERVERS - 1) + "...");
    serverInstance.get(SERVERS - 1).startServer(getDistributedServerConfiguration(serverInstance.get(SERVERS - 1)));
    if (serverInstance.get(SERVERS - 1).server.getPluginByClass(OHazelcastPlugin.class) != null)
      serverInstance.get(SERVERS - 1).server.getPluginByClass(OHazelcastPlugin.class).waitUntilOnline();

    Thread.sleep(1000);

    banner("RESTARTING TESTS WITH SERVER " + (SERVERS - 1) + " UP...");

    executeMultipleTest();
  }

  @Override
  protected void onBeforeChecks() throws InterruptedException {
    // // WAIT UNTIL THE END
    waitFor(2, new OCallable<Boolean, ODatabaseDocumentTx>() {
      @Override
      public Boolean call(ODatabaseDocumentTx db) {
        final boolean ok = db.countClass("Person") >= count * writerCount * SERVERS + baseCount;
        if (!ok)
          System.out.println(
              "FOUND " + db.countClass("Person") + " people instead of expected " + (count * writerCount * SERVERS) + baseCount);
        return ok;
      }
    }, 10000);
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  public String getDatabaseName() {
    return "distributed-hatxtest";
  }
}

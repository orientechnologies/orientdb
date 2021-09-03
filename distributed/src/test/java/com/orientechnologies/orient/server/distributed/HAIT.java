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

import com.orientechnologies.orient.setup.ServerRun;
import org.junit.Ignore;
import org.junit.Test;

/** Distributed TX test against "plocal" protocol + shutdown and restart of a node. */
public class HAIT extends AbstractHARemoveNode {
  static final int SERVERS = 3;

  @Test
  @Ignore
  public void test() throws Exception {
    useTransactions = false;
    count = 10;
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void onAfterExecution() throws Exception {
    banner("SIMULATE SOFT SHUTDOWN OF SERVER " + (SERVERS - 1));
    serverInstance.get(SERVERS - 1).shutdownServer();

    banner("RESTARTING TESTS WITH SERVER " + (SERVERS - 1) + " DOWN...");

    count = 10;

    executeMultipleTest();

    banner("RESTARTING SERVER " + (SERVERS - 1) + "...");

    serverInstance
        .get(SERVERS - 1)
        .startServer(getDistributedServerConfiguration(serverInstance.get(SERVERS - 1)));
    if (serverInstance.get(SERVERS - 1).getServerInstance().getDistributedManager() != null)
      serverInstance
          .get(SERVERS - 1)
          .getServerInstance()
          .getDistributedManager()
          .waitUntilNodeOnline();

    banner("RESTARTING TESTS WITH SERVER " + (SERVERS - 1) + " UP...");

    count = 10;

    executeMultipleTest();
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  public String getDatabaseName() {
    return "distributed-hatest";
  }
}

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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.setup.ServerRun;
import java.util.HashSet;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

/**
 * Checks the listeners are correctly invoked at every status change of databases even after a
 * restart.
 */
public class DistributedListenerIT extends AbstractServerClusterTxTest {
  private static final int SERVERS = 2;
  private volatile boolean restartExecuted = false;
  private final Set<String> afterRestartdbOnline = new HashSet<>();

  @Test
  public void test() throws Exception {
    startupNodesInSequence = true;
    count = 10;
    maxRetries = 10;
    init(SERVERS);
    prepare(false);

    execute();
  }

  @Override
  protected void onServerStarted(final ServerRun server) {
    super.onServerStarted(server);

    // INSTALL ON FIRST SERVER ONLY THE SERVER MONITOR TO CHECK IF HAS BEEN RESTARTED
    server
        .server
        .getDistributedManager()
        .registerLifecycleListener(
            new ODistributedLifecycleListener() {
              @Override
              public boolean onNodeJoining(String iNode) {
                return true;
              }

              @Override
              public void onNodeJoined(String iNode) {}

              @Override
              public void onNodeLeft(String iNode) {}

              public void onDatabaseChangeStatus(
                  String iNode,
                  String iDatabaseName,
                  ODistributedServerManager.DB_STATUS iNewStatus) {
                OLogManager.instance()
                    .info(this, "Node %s DB %s Status %s", null, iNode, iDatabaseName, iNewStatus);

                if (iNewStatus == ODistributedServerManager.DB_STATUS.ONLINE) {
                  final String dbName = iNode + ":" + iDatabaseName;
                  if (restartExecuted) afterRestartdbOnline.add(dbName);
                }
              }
            });
  }

  @Override
  protected void onAfterExecution() throws Exception {
    restartExecuted = true;

    // BACKUP LAST SERVER, RUN ASYNCHRONOUSLY
    serverInstance.get(0).shutdownServer();
    serverInstance.get(1).shutdownServer();

    banner("RESTART OF SERVERS");

    try {
      startServers();
    } catch (Exception e) {
      Assertions.fail(e.toString());
    }

    waitForDatabaseIsOnline(
        0,
        serverInstance.get(0).getServerInstance().getDistributedManager().getLocalNodeName(),
        getDatabaseName(),
        30000);
    waitForDatabaseIsOnline(
        1,
        serverInstance.get(1).getServerInstance().getDistributedManager().getLocalNodeName(),
        getDatabaseName(),
        30000);

    Assert.assertTrue(
        "DB online after restart " + afterRestartdbOnline, !afterRestartdbOnline.isEmpty());
  }

  @Override
  public String getDatabaseName() {
    return "distributed-listener";
  }
}

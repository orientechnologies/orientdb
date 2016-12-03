/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *  
 */

package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests the behavior of hooks in distributed configuration.
 */
public class DistributedLifecycleListenerTest extends AbstractServerClusterTest implements ODistributedLifecycleListener {
  private final static int                                               SERVERS        = 2;

  private final AtomicLong                                               beforeNodeJoin = new AtomicLong();
  private final AtomicLong                                               afterNodeJoin  = new AtomicLong();
  private final AtomicLong                                               nodeLeft       = new AtomicLong();
  private final List<OPair<String, ODistributedServerManager.DB_STATUS>> changeStatus   = Collections
      .synchronizedList(new ArrayList<OPair<String, ODistributedServerManager.DB_STATUS>>());

  @Override
  public boolean onNodeJoining(String iNode) {
    beforeNodeJoin.incrementAndGet();
    return true;
  }

  @Override
  public void onNodeJoined(String iNode) {
    afterNodeJoin.incrementAndGet();
  }

  @Override
  public void onNodeLeft(String iNode) {
    nodeLeft.incrementAndGet();
  }

  @Override
  public void onDatabaseChangeStatus(String iNode, String iDatabaseName, ODistributedServerManager.DB_STATUS iNewStatus) {
    OLogManager.instance().info(this, "CHANGE OF STATUS node=%s db=%s status-%s", iNode, iDatabaseName, iNewStatus);
    changeStatus.add(new OPair<String, ODistributedServerManager.DB_STATUS>(iNode + "." + iDatabaseName, iNewStatus));
  }

  public String getDatabaseName() {
    return "distributed-lifecycle";
  }

  @Test
  public void test() throws Exception {
    this.startupNodesInSequence = true;
    this.terminateAtShutdown = false;
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void onServerStarted(ServerRun server) {
    server.getServerInstance().getDistributedManager().registerLifecycleListener(this);
  }

  @Override
  protected void executeTest() throws Exception {
    Thread.sleep(1000);
  }

  @Override
  protected void onTestEnded() {
    Assert.assertEquals(SERVERS - 1, beforeNodeJoin.get());
    Assert.assertEquals(SERVERS - 1, afterNodeJoin.get());

    for (int attempt = 0; attempt < 50; ++attempt) {
      if (nodeLeft.get() >= SERVERS - 1)
        break;
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    Assert.assertEquals(SERVERS - 1, nodeLeft.get());

    Assert.assertEquals(9, changeStatus.size());
    // Assert.assertEquals("europe-0." + getDatabaseName(), changeStatus.get(0).getKey());
    // Assert.assertEquals(ODistributedServerManager.DB_STATUS.BACKUP, changeStatus.get(0).getValue());
    // Assert.assertEquals("europe-0." + getDatabaseName(), changeStatus.get(1).getKey());
    // Assert.assertEquals(ODistributedServerManager.DB_STATUS.ONLINE, changeStatus.get(1).getValue());
  }
}

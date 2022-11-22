/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.server.distributed;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.setup.ServerRun;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;

/** Tests the behavior of hooks in distributed configuration. */
public class DistributedLifecycleListenerIT extends AbstractServerClusterTest
    implements ODistributedLifecycleListener {
  private static final int SERVERS = 2;

  private final AtomicLong beforeNodeJoin = new AtomicLong();
  private final AtomicLong afterNodeJoin = new AtomicLong();
  private final AtomicLong nodeLeft = new AtomicLong();
  private final List<OPair<String, ODistributedServerManager.DB_STATUS>> changeStatus =
      Collections.synchronizedList(
          new ArrayList<OPair<String, ODistributedServerManager.DB_STATUS>>());
  private final CountDownLatch started = new CountDownLatch(SERVERS - 1);
  private final CountDownLatch ended = new CountDownLatch(SERVERS - 1);

  @Override
  public boolean onNodeJoining(String iNode) {
    beforeNodeJoin.incrementAndGet();
    return true;
  }

  @Override
  public void onNodeJoined(String iNode) {
    afterNodeJoin.incrementAndGet();
    started.countDown();
  }

  @Override
  public void onNodeLeft(String iNode) {
    nodeLeft.incrementAndGet();
    ended.countDown();
  }

  @Override
  public void onDatabaseChangeStatus(
      String iNode, String iDatabaseName, ODistributedServerManager.DB_STATUS iNewStatus) {
    OLogManager.instance()
        .info(this, "CHANGE OF STATUS node=%s db=%s status-%s", iNode, iDatabaseName, iNewStatus);
    changeStatus.add(
        new OPair<String, ODistributedServerManager.DB_STATUS>(
            iNode + "." + iDatabaseName, iNewStatus));
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
    assertTrue(started.await(5, TimeUnit.SECONDS));
  }

  @Override
  protected void onTestEnded() {
    Assert.assertEquals(SERVERS - 1, beforeNodeJoin.get());
    Assert.assertEquals(SERVERS - 1, afterNodeJoin.get());

    try {
      assertTrue(ended.await(5, TimeUnit.SECONDS));
    } catch (InterruptedException e1) {
      fail();
    }
    Assert.assertEquals(SERVERS - 1, nodeLeft.get());

    Assert.assertEquals(10, changeStatus.size());
    // Assert.assertEquals("europe-0." + getDatabaseName(), changeStatus.get(0).getKey());
    // Assert.assertEquals(ODistributedServerManager.DB_STATUS.BACKUP,
    // changeStatus.get(0).getValue());
    // Assert.assertEquals("europe-0." + getDatabaseName(), changeStatus.get(1).getKey());
    // Assert.assertEquals(ODistributedServerManager.DB_STATUS.ONLINE,
    // changeStatus.get(1).getValue());
  }
}

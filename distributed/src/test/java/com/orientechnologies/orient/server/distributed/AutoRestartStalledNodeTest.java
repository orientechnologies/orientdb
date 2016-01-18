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

import com.hazelcast.core.IQueue;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.server.hazelcast.OHazelcastDistributedMessageService;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;

/**
 * Starts 3 servers, lock on node3 simulating a unknown stall situation, checks:
 * <ul>
 * <li>other nodes can work in the meanwhile</li>
 * <li>node3 is restarted automatically</li>
 * <li>node3 is realigned once backup is finished</li>
 * </ul>
 */
public class AutoRestartStalledNodeTest extends AbstractServerClusterTxTest {
  final static int SERVERS       = 3;
  protected Timer  timer         = new Timer(true);
  volatile boolean inserting     = true;
  volatile int     serverStarted = 0;
  volatile boolean nodeStalled   = false;

  @Test
  public void test() throws Exception {
    // SET MAXQUEUE SIZE LOWER TO TEST THE NODE IS RESTARTED AUTOMATICALLY
    final long queueMaxSize = OGlobalConfiguration.DISTRIBUTED_QUEUE_MAXSIZE.getValueAsLong();
    OGlobalConfiguration.DISTRIBUTED_QUEUE_MAXSIZE.setValue(9);

    try {
      startupNodesInSequence = true;
      useTransactions = false;
      count = 500;
      maxRetries = 10;
      init(SERVERS);
      prepare(false);

      // EXECUTE TESTS ONLY ON FIRST 2 NODES LEAVING NODE3 AD BACKUP ONLY REPLICA
      executeTestsOnServers = new ArrayList<ServerRun>();
      for (int i = 0; i < serverInstance.size() - 1; ++i) {
        executeTestsOnServers.add(serverInstance.get(i));
      }

      execute();

    } finally {
      OGlobalConfiguration.DISTRIBUTED_QUEUE_MAXSIZE.setValue(queueMaxSize);
    }
  }

  @Override
  protected void onServerStarted(ServerRun server) {
    super.onServerStarted(server);

    if (serverStarted++ == (SERVERS - 1)) {
      new Timer(true).schedule(new TimerTask() {
        @Override
        public void run() {
          // DUMP QUEUE SIZES
          System.out.println("---------------------------------------------------------------------");
          for (int i = 0; i < SERVERS; ++i) {
            try {
              final OHazelcastPlugin dInstance = (OHazelcastPlugin) serverInstance.get(i).getServerInstance()
                  .getDistributedManager();

              final String queueName = OHazelcastDistributedMessageService.getRequestQueueName(dInstance.getLocalNodeName(),
                  getDatabaseName());
              final IQueue<Object> queue = dInstance.getMessageService().getQueue(queueName);

              System.out.println("Queue " + queueName + " size = " + queue.size());
            } catch (Exception e) {
            }
          }
          System.out.println("---------------------------------------------------------------------");
        }
      }, 1000, 1000);

      // BACKUP LAST SERVER IN 3 SECONDS
      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          Assert.assertTrue("Insert was too fast", inserting);

          banner("STARTING LOCKING SERVER " + (SERVERS - 1));

          OrientGraphFactory factory = new OrientGraphFactory(
              "plocal:target/server" + (SERVERS - 1) + "/databases/" + getDatabaseName());
          OrientGraphNoTx g = factory.getNoTx();

          nodeStalled = true;
          try {
            final OAbstractPaginatedStorage stg = (OAbstractPaginatedStorage) g.getRawGraph().getStorage().getUnderlying();
            stg.callInLock(new Callable<Object>() {
              @Override
              public Object call() throws Exception {

                // SIMULATE LONG WAIT
                Thread.sleep(3000);

                return null;
              }
            }, true);

          } finally {
            banner("RELEASED STALLED SERVER " + (SERVERS - 1));
            nodeStalled = false;
          }
        }
      }, 5000);
    }
  }

  @Override
  protected void onAfterExecution() throws Exception {
    inserting = false;
    Assert.assertFalse(nodeStalled);
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "remote:" + server.getBinaryProtocolAddress() + "/" + getDatabaseName();
  }

  @Override
  public String getDatabaseName() {
    return "distributed-autorestartstallednode";
  }
}

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

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Starts 3 servers, lock on node3 simulating a unknown stall situation, checks:
 * <ul>
 * <li>other nodes can work in the meanwhile</li>
 * <li>node3 is restarted automatically</li>
 * <li>node3 is realigned once backup is finished</li>
 * </ul>
 */
public class AutoRestartStalledNodeTest extends AbstractServerClusterTxTest {
  final static int            SERVERS       = 3;
  volatile boolean            inserting     = true;
  volatile int                serverStarted = 0;
  volatile boolean            nodeStalled   = false;
  final private AtomicInteger nodeLefts     = new AtomicInteger();

  @Test
  public void test() throws Exception {
    // SET MAXQUEUE SIZE LOWER TO TEST THE NODE IS RESTARTED AUTOMATICALLY
    final long queueMaxSize = OGlobalConfiguration.DISTRIBUTED_QUEUE_MAXSIZE.getValueAsLong();
    OGlobalConfiguration.DISTRIBUTED_QUEUE_MAXSIZE.setValue(100);

    try {
      startupNodesInSequence = true;
      useTransactions = false;
      count = 400;
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

    if (serverStarted == 0) {
      // INSTALL ON FIRST SERVER ONLY THE SERVER MONITOR TO CHECK IF HAS BEEN RESTARTED
      server.server.getDistributedManager().registerLifecycleListener(new ODistributedLifecycleListener() {
        @Override
        public boolean onNodeJoining(String iNode) {
          return true;
        }

        @Override
        public void onNodeJoined(String iNode) {
        }

        @Override
        public void onNodeLeft(String iNode) {
          nodeLefts.incrementAndGet();
        }
      });
    }

    if (serverStarted++ == (SERVERS - 1)) {

      startQueueMonitorTask();

      // BACKUP LAST SERVER, RUN ASYNCHRONOUSLY
      new Thread(new Runnable() {

        @Override
        public void run() {
          try {
            // CRASH LAST SERVER try {
            executeWhen(new Callable<Boolean>() {
              // CONDITION
              @Override
              public Boolean call() throws Exception {
                final ODatabaseDocumentTx database = poolFactory.get(getDatabaseURL(serverInstance.get(0)), "admin", "admin")
                    .acquire();
                try {
                  return database.countClass("Person") > (count * SERVERS) * 1 / 3;
                } finally {
                  database.close();
                }
              }
            }, // ACTION
                new Callable() {
              @Override
              public Object call() throws Exception {
                Assert.assertTrue("Insert was too fast", inserting);

                banner("STARTING LOCKING SERVER " + (SERVERS - 1));

                OrientGraphFactory factory = new OrientGraphFactory(// getDatabaseURL(serverInstance.get(SERVERS - 2)));
                    "plocal:target/server" + (SERVERS - 1) + "/databases/" + getDatabaseName());
                OrientGraphNoTx g = factory.getNoTx();

                nodeStalled = true;
                try {
                  g.getRawGraph().getStorage().callInLock(new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {

                      // SIMULATE LONG BACKUP
                      Thread.sleep(10000);

                      return null;
                    }
                  }, true);

                } finally {
                  g.shutdown();

                  banner("RELEASED STALLED SERVER " + (SERVERS - 1));
                  nodeStalled = false;
                }
                return null;
              }
            });

          } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("Error on execution flow");
          }
        }

      }).start();
    }
  }

  @Override
  protected void onAfterExecution() throws Exception {
    inserting = false;
    Assert.assertFalse(nodeStalled);
    Assert.assertEquals("Found no node has been restarted", 1, nodeLefts.get());
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "remote:" + server.getBinaryProtocolAddress() + "/" + getDatabaseName();
  }

  @Override
  public String getDatabaseName() {
    return "distributed-autorestartstallednode";
  }
}

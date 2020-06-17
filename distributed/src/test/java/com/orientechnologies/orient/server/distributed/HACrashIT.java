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
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Distributed non TX test against "remote" protocol. It starts 3 servers and during a stress test,
 * kill last server. The test checks all the clients can auto-reconnect to the next available
 * server.
 */
public class HACrashIT extends AbstractServerClusterTxTest {
  private static final int SERVERS = 3;
  private volatile boolean inserting = true;
  private volatile int serverStarted = 0;
  private volatile boolean serverRestarted = false;

  @Test
  @Ignore
  public void test() throws Exception {
    startupNodesInSequence = true;
    count = 500;
    maxRetries = 10;
    delayWriter = 0;
    useTransactions = false;
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void onServerStarted(ServerRun server) {
    super.onServerStarted(server);

    if (serverStarted++ == (SERVERS - 1)) {
      // RUN ASYNCHRONOUSLY
      new Thread(
              new Runnable() {
                @Override
                public void run() {
                  try {
                    // CRASH LAST SERVER try {
                    executeWhen(
                        0,
                        new OCallable<Boolean, ODatabaseDocument>() {
                          // CONDITION
                          @Override
                          public Boolean call(ODatabaseDocument db) {
                            return db.countClass("Person")
                                > (count * SERVERS * writerCount + baseCount) * 1 / 3;
                          }
                        }, // ACTION
                        new OCallable<Boolean, ODatabaseDocument>() {
                          @Override
                          public Boolean call(ODatabaseDocument db) {
                            Assert.assertTrue("Insert was too fast", inserting);
                            banner("SIMULATE FAILURE ON SERVER " + (SERVERS - 1));

                            delayWriter = 50;
                            serverInstance.get(SERVERS - 1).crashServer();

                            executeWhen(
                                db,
                                new OCallable<Boolean, ODatabaseDocument>() {
                                  @Override
                                  public Boolean call(ODatabaseDocument db) {
                                    return db.countClass("Person")
                                        > (count * writerCount * SERVERS) * 2 / 4;
                                  }
                                },
                                new OCallable<Boolean, ODatabaseDocument>() {
                                  @Override
                                  public Boolean call(ODatabaseDocument db) {
                                    Assert.assertTrue("Insert was too fast", inserting);

                                    banner("RESTARTING SERVER " + (SERVERS - 1) + "...");
                                    try {
                                      serverInstance
                                          .get(SERVERS - 1)
                                          .startServer(
                                              getDistributedServerConfiguration(
                                                  serverInstance.get(SERVERS - 1)));
                                      serverRestarted = true;
                                      delayWriter = 0;

                                    } catch (Exception e) {
                                      e.printStackTrace();
                                    }
                                    return null;
                                  }
                                });
                            return null;
                          }
                        });

                  } catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail("Error on execution flow");
                  }
                }
              })
          .start();
    }
  }

  @Override
  protected void onBeforeChecks() throws InterruptedException {
    // // WAIT UNTIL THE END
    waitFor(
        2,
        new OCallable<Boolean, ODatabaseDocument>() {
          @Override
          public Boolean call(ODatabaseDocument db) {
            final boolean ok = db.countClass("Person") >= count * writerCount * SERVERS + baseCount;
            if (!ok)
              System.out.println(
                  "FOUND "
                      + db.countClass("Person")
                      + " people instead of expected "
                      + (count * writerCount * SERVERS)
                      + baseCount);
            return ok;
          }
        },
        10000);
  }

  @Override
  protected void onAfterExecution() throws Exception {
    inserting = false;

    waitFor(
        20000,
        new OCallable<Boolean, Void>() {
          @Override
          public Boolean call(Void iArgument) {
            return serverRestarted;
          }
        },
        "Server 2 is not active yet");

    banner("CHECKING IF NODE 2 IS STILL ACTIVE");
    Assert.assertTrue(serverRestarted);
  }

  protected String getDatabaseURL(final ServerRun server) {
    final String address = server.getBinaryProtocolAddress();

    if (address == null) return null;

    return "remote:" + address + "/" + getDatabaseName();
  }

  @Override
  public String getDatabaseName() {
    return "distributed-txhacrash";
  }
}

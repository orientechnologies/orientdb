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
import com.orientechnologies.orient.setup.ServerRun;
import java.util.concurrent.Callable;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Distributed TX test against "remote" protocol. It starts 3 servers and during a stress test, kill
 * last server. The test checks all the clients can auto-reconnect to the next available server.
 */
public class HATxCrashIT extends AbstractHARemoveNode {
  private static final int SERVERS = 3;
  private volatile boolean inserting = true;
  private volatile int serverStarted = 0;
  private volatile boolean lastServerOn = false;

  @Test
  @Ignore
  public void test() throws Exception {
    startupNodesInSequence = true;
    count = 500;
    maxRetries = 10;
    useTransactions = true;
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected void onServerStarted(ServerRun server) {
    super.onServerStarted(server);

    if (serverStarted++ == (SERVERS - 1)) {
      lastServerOn = true;

      // RUN ASYNCHRONOUSLY
      new Thread(
              new Runnable() {
                @Override
                public void run() {
                  try {
                    // CRASH LAST SERVER try {
                    executeWhen(
                        new Callable<Boolean>() {
                          // CONDITION
                          @Override
                          public Boolean call() throws Exception {
                            // Why did this in the poolFactory version always use
                            // serverInstance.get(0)...?
                            // Does it affect the test to use a specific server?
                            final ODatabaseDocument database = getDatabase(server);
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
                            banner("SIMULATE FAILURE ON SERVER " + (SERVERS - 1));
                            delayWriter = 100;
                            serverInstance.get(SERVERS - 1).crashServer();
                            //                    poolFactory.reset();
                            lastServerOn = false;

                            executeWhen(
                                new Callable<Boolean>() {
                                  @Override
                                  public Boolean call() throws Exception {
                                    // Why did this in the poolFactory version always use
                                    // serverInstance.get(0)...?
                                    // Does it affect the test to use a specific server?
                                    ServerRun runningServer = null;
                                    for (ServerRun run : serverInstance) {
                                      if (run.getServerInstance().getDatabases().isOpen()) {
                                        runningServer = run;
                                        break;
                                      }
                                    }
                                    final ODatabaseDocument database = getDatabase(runningServer);

                                    try {
                                      return database.countClass("Person")
                                          > (count * SERVERS) * 2 / 3;
                                    } finally {
                                      database.close();
                                    }
                                  }
                                },
                                new Callable() {
                                  @Override
                                  public Object call() throws Exception {
                                    Assert.assertTrue("Insert was too fast", inserting);

                                    banner("RESTARTING SERVER " + (SERVERS - 1) + "...");
                                    try {
                                      serverInstance
                                          .get(SERVERS - 1)
                                          .startServer(
                                              getDistributedServerConfiguration(
                                                  serverInstance.get(SERVERS - 1)));
                                      lastServerOn = true;
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
  protected void onAfterExecution() throws Exception {
    inserting = false;

    waitFor(
        5000,
        new OCallable<Boolean, Void>() {
          @Override
          public Boolean call(Void iArgument) {
            return lastServerOn;
          }
        },
        "Server 2 is not active yet");

    Assert.assertTrue(lastServerOn);
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

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

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Callable;

/**
 * Distributed TX test against "remote" protocol. It starts 3 servers and during a stress test, kill last server. The test checks
 * all the clients can auto-reconnect to the next available server.
 */
public class HACrashTest extends AbstractServerClusterTxTest {
  final static int SERVERS       = 3;
  volatile boolean inserting     = true;
  volatile int     serverStarted = 0;
  volatile boolean lastServerOn  = false;

  @Test
  public void test() throws Exception {
    startupNodesInSequence = true;
    count = 500;
    maxRetries = 10;
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
                banner("SIMULATE FAILURE ON SERVER " + (SERVERS - 1));
                serverInstance.get(SERVERS - 1).crashServer();
                lastServerOn = false;

                executeWhen(new Callable<Boolean>() {
                  @Override
                  public Boolean call() throws Exception {
                    final ODatabaseDocumentTx database = poolFactory.get(getDatabaseURL(serverInstance.get(0)), "admin", "admin")
                        .acquire();
                    try {
                      return database.countClass("Person") > (count * SERVERS) * 2 / 3;
                    } finally {
                      database.close();
                    }
                  }
                }, new Callable() {
                  @Override
                  public Object call() throws Exception {
                    Assert.assertTrue("Insert was too fast", inserting);

                    banner("RESTARTING SERVER " + (SERVERS - 1) + "...");
                    try {
                      serverInstance.get(SERVERS - 1)
                          .startServer(getDistributedServerConfiguration(serverInstance.get(SERVERS - 1)));
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
      }).start();
    }
  }

  @Override
  protected void onAfterExecution() throws Exception {
    inserting = false;
    Assert.assertTrue(lastServerOn);
  }

  protected String getDatabaseURL(final ServerRun server) {
    return "remote:" + server.getBinaryProtocolAddress() + "/" + getDatabaseName();
  }

  @Override
  public String getDatabaseName() {
    return "distributed-txhacrash";
  }
}

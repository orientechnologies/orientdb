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

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Starts 3 servers, backup on node3, check other nodes can work in the meanwhile and node3 is
 * realigned once backup is finished. No automatic restart must be executed.
 */
public class OneNodeBackupIT extends AbstractServerClusterTxTest {
  static final int SERVERS = 3;
  volatile boolean inserting = true;
  volatile int serverStarted = 0;
  volatile boolean backupInProgress = false;
  final AtomicInteger nodeLefts = new AtomicInteger();

  @Test
  @Ignore
  public void test() throws Exception {
    startupNodesInSequence = true;
    count = 1000;
    maxRetries = 10;
    init(SERVERS);
    prepare(false);

    // EXECUTE TESTS ONLY ON FIRST 2 NODES LEAVING NODE3 AS BACKUP ONLY REPLICA
    executeTestsOnServers = new ArrayList<ServerRun>();
    for (int i = 0; i < serverInstance.size() - 1; ++i) {
      executeTestsOnServers.add(serverInstance.get(i));
    }

    execute();
  }

  @Override
  protected void onServerStarted(ServerRun server) {
    super.onServerStarted(server);

    if (serverStarted == 0) {
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
                public void onNodeLeft(String iNode) {
                  nodeLefts.incrementAndGet();
                }

                @Override
                public void onDatabaseChangeStatus(
                    String iNode,
                    String iDatabaseName,
                    ODistributedServerManager.DB_STATUS iNewStatus) {}
              });
    }

    if (serverStarted++ == (SERVERS - 1)) {
      // BACKUP LAST SERVER, RUN ASYNCHRONOUSLY
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
                            final ODatabaseDocument database = getDatabase(0);
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

                            banner("STARTING BACKUP SERVER " + (SERVERS - 1));

                            ODatabaseDocument g = getDatabase(SERVERS - 1);
                            if (databaseExists(SERVERS - 1)) {
                              g = getDatabase(SERVERS - 1);
                            } else {
                              createDatabase(SERVERS - 1);
                            }

                            backupInProgress = true;
                            File file = null;
                            try {
                              file = File.createTempFile("orientdb_test_backup", ".zip");
                              if (file.exists()) Assert.assertTrue(file.delete());

                              g.backup(
                                  new FileOutputStream(file),
                                  null,
                                  new Callable<Object>() {
                                    @Override
                                    public Object call() throws Exception {

                                      // SIMULATE LONG BACKUP
                                      Thread.sleep(10000);

                                      return null;
                                    }
                                  },
                                  null,
                                  9,
                                  1000000);

                            } catch (IOException e) {
                              e.printStackTrace();
                            } finally {
                              banner("COMPLETED BACKUP SERVER " + (SERVERS - 1));
                              backupInProgress = false;

                              if (file != null) file.delete();

                              g.close();
                            }
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
    Assert.assertFalse(backupInProgress);
    Assert.assertEquals("Found some nodes has been restarted", 0, nodeLefts.get());
  }

  @Override
  public String getDatabaseName() {
    return "distributed-backup1node";
  }
}

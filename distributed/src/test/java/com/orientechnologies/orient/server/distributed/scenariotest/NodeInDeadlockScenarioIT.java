/*
 * Copyright 2015 OrientDB LTD (info--at--orientdb.com)
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

package com.orientechnologies.orient.server.distributed.scenariotest;

import static org.junit.Assert.fail;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.setup.ServerRun;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.junit.Assert;
import org.junit.Test;

/**
 * It checks the consistency in the cluster with the following scenario: - 3 server (quorum=2) - 5
 * threads write 100 records on server1 and server2 - meanwhile after 1/3 of to-write records
 * server3 goes in deadlock (backup), and after 2/3 of to-write records goes up. - check that
 * changes are propagated on server2 - deadlock-ending on server3 - after a while check that last
 * changes are propagated on server3.
 *
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 */
public class NodeInDeadlockScenarioIT extends AbstractScenarioTest {

  volatile boolean inserting = true;
  volatile int serverStarted = 0;
  volatile boolean backupInProgress = false;

  @Test
  public void test() throws Exception {

    maxRetries = 10;
    init(SERVERS);
    prepare(false);

    // EXECUTE TESTS ONLY ON FIRST 2 NODES LEAVING NODE3 AD BACKUP ONLY REPLICA
    executeTestsOnServers = new ArrayList<ServerRun>();
    for (int i = 0; i < serverInstance.size() - 1; ++i) {
      executeTestsOnServers.add(serverInstance.get(i));
    }

    execute();
  }

  @Override
  public void executeTest() throws Exception {

    List<ODocument> result = null;
    ODatabaseDocument dbServer3 = getDatabase(2);

    try {

      /*
       * Test with quorum = 2
       */

      banner("Test with quorum = 2");

      // writes on server1 and server2
      executeMultipleWrites(this.executeTestsOnServers, "remote");

      // check consistency on server1 and server2
      checkWritesAboveCluster(executeTestsOnServers, executeTestsOnServers);

      // waiting for server3 releasing
      while (backupInProgress == true) {
        Thread.sleep(1000);
      }

      // check consistency on all the server
      checkWritesAboveCluster(serverInstance, executeTestsOnServers);

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    } finally {

      if (!dbServer3.isClosed()) {
        dbServer3.activateOnCurrentThread();
        dbServer3.close();
      }
    }
  }

  @Override
  protected void onServerStarted(ServerRun server) {
    super.onServerStarted(server);

    if (serverStarted == 0) startCountMonitorTask("Person");

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
                              long recordCount = database.countClass("Person");
                              boolean condition =
                                  recordCount
                                      > (count * writerCount * (SERVERS - 1) + baseCount) * 1 / 3;
                              return condition;
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

                            ODatabaseDocument g = null;
                            if (databaseExists(SERVERS - 1)) {
                              g = getDatabase(SERVERS - 1);
                            } else {
                              createDatabase(SERVERS - 1);
                              g = getDatabase(SERVERS - 1);
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

                                      Thread.sleep(5000);

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

                              g.close();

                              if (file != null) file.delete();
                            }
                            return null;
                          }
                        });

                  } catch (Exception e) {
                    e.printStackTrace();
                    fail("Error on execution flow");
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
            final boolean ok =
                db.countClass("Person") >= count * writerCount * (SERVERS - 1) + baseCount;
            if (!ok)
              System.out.println(
                  "FOUND "
                      + db.countClass("Person")
                      + " people instead of expected "
                      + (count * writerCount * (SERVERS - 1) + baseCount));
            return ok;
          }
        },
        10000);
  }

  @Override
  protected void onAfterExecution() throws Exception {
    inserting = false;
    Assert.assertFalse(backupInProgress);
  }

  @Override
  public String getDatabaseName() {
    return "distributed-node-deadlock";
  }
}

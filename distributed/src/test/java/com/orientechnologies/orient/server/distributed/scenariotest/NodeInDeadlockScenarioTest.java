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

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedLifecycleListener;
import com.orientechnologies.orient.server.distributed.ServerRun;
import com.orientechnologies.orient.server.distributed.scenariotest.AbstractScenarioTest;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * It checks the consistency in the cluster with the following scenario:
 * - 3 server (quorum=2)
 * - 5 threads write 100 records on server1 and server2
 * - meanwhile after 1/3 of to-write records server3 goes in deadlock (backup), and after 2/3 of to-write records goes up.
 * - check that changes are propagated on server2
 * - deadlock-ending on server3
 * - after a while check that last changes are propagated on server3.
 */

public class NodeInDeadlockScenarioTest extends AbstractScenarioTest {

  protected Timer timer             = new Timer(true);
  volatile boolean inserting        = true;
  volatile int     serverStarted    = 0;
  volatile boolean backupInProgress = false;

  @Test
  public void test() throws Exception {

    maxRetries = 10;
    init(SERVERS);
    prepare(false);

    // EXECUTE TESTS ONLY ON FIRST 2 NODES LEAVING NODE3 AD BACKUP ONLY REPLICA
    executeWritesOnServers = new ArrayList<ServerRun>();
    for (int i = 0; i < serverInstance.size() - 1; ++i) {
      executeWritesOnServers.add(serverInstance.get(i));
    }

    execute();
  }

  @Override
  public void executeTest() throws Exception {

    List<ODocument> result = null;
    ODatabaseDocumentTx dbServer3 = new ODatabaseDocumentTx(getRemoteDatabaseURL(serverInstance.get(2))).open("admin", "admin");

    try {

      /*
       * Test with quorum = 2
       */

      banner("Test with quorum = 2");

      // writes on server1 and server2
      ODatabaseRecordThreadLocal.INSTANCE.set(null);
      executeMultipleWrites(this.executeWritesOnServers,"remote");

      // check consistency on server1 and server2
      checkWritesAboveCluster(executeWritesOnServers, executeWritesOnServers);

      // waiting for server3 releasing
      while(backupInProgress==true) {
        Thread.sleep(1000);
      }

      // waiting for changes propagation
      Thread.sleep(1000);

      // check consistency on all the server
      checkWritesAboveCluster(serverInstance, executeWritesOnServers);

    } catch(Exception e) {
      e.printStackTrace();
      assertTrue(false);
    } finally {

      if(!dbServer3.isClosed()) {
        ODatabaseRecordThreadLocal.INSTANCE.set(dbServer3);
        dbServer3.close();
        ODatabaseRecordThreadLocal.INSTANCE.set(null);
      }
    }
  }

  @Override
  protected void onServerStarted(ServerRun server) {
    super.onServerStarted(server);

    if (serverStarted++ == (SERVERS - 1)) {
      startQueueMonitorTask();
      startCountMonitorTask("Person");

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
                              long recordCount = database.countClass("Person");
                              boolean condition = recordCount > (count * writerCount) * 1 / 3;
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

                    OrientGraphFactory factory = new OrientGraphFactory(
                        "plocal:target/server" + (SERVERS - 1) + "/databases/" + getDatabaseName());
                    OrientGraphNoTx g = factory.getNoTx();

                    backupInProgress = true;
                    File file = null;
                    try {
                      file = File.createTempFile("orientdb_test_backup", ".zip");
                      if (file.exists())
                        Assert.assertTrue(file.delete());

                      g.getRawGraph().backup(new FileOutputStream(file), null, new Callable<Object>() {
                        @Override
                        public Object call() throws Exception {

                          // SIMULATE LONG BACKUP UP TO 2/3 OF RECORDS
                          while (totalVertices.get() < (count * SERVERS) * 2 / 3) {
                            Thread.sleep(1000);
                          }

                          return null;
                        }
                      }, null, 9, 1000000);

                    } catch (IOException e) {
                      e.printStackTrace();
                    } finally {
                      banner("COMPLETED BACKUP SERVER " + (SERVERS - 1));
                      backupInProgress = false;

                      g.shutdown();

                      if (file != null)
                        file.delete();
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
    Assert.assertFalse(backupInProgress);
  }

}

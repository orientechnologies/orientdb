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
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ServerRun;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * It checks the consistency in the cluster with the following scenario:
 * - 3 server (quorum=1)
 * - deadlock on server3
 * - record r1 inserted on server1
 * - record r1 (version x) is present in full replica on all the servers
 * - client c1 (connected to server1) updates r1 with the value v1, meanwhile client c2 (connected to server2) updates r1 with value v2
 * - end of deadlock on server3
 * - check consistency on the nodes:
 *      - r1 has the same value on all the servers
 *      - r1 has version x+1 on all the servers
 */

public class WWConflictAndNodeInDeadlockScenarioTest extends UpdateConflictFixTaskScenarioTest {

  protected  Timer   timer            = new Timer(true);
  volatile int     serverStarted    = 0;
  volatile boolean backupInProgress = false;
  private volatile boolean server3inDeadlock = false;


  @Ignore
  @Test
  public void test() throws Exception {

    maxRetries = 10;
    init(SERVERS);
    prepare(false);

    // execute writes only on server3
    executeWritesOnServers.addAll(serverInstance);

    execute();
  }

  @Override
  public void executeTest() throws Exception {

    /*
     * Test with quorum = 1
     */

    banner("Test with quorum = 1");

    ODatabaseDocumentTx dbServer1 = poolFactory.get(getDatabaseURL(serverInstance.get(0)), "admin", "admin").acquire();
    ODatabaseDocumentTx dbServer2 = poolFactory.get(getDatabaseURL(serverInstance.get(1)), "admin", "admin").acquire();
    ODatabaseDocumentTx dbServer3 = poolFactory.get(getDatabaseURL(serverInstance.get(2)), "admin", "admin").acquire();

    // changing configuration: writeQuorum=1, autoDeploy=false
    System.out.print("\nChanging configuration (writeQuorum=1, autoDeploy=false)...");

    ODocument cfg = null;
    ServerRun server = serverInstance.get(2);
    OHazelcastPlugin manager = (OHazelcastPlugin) server.getServerInstance().getDistributedManager();
    ODistributedConfiguration databaseConfiguration = manager.getDatabaseConfiguration("distributed-inserttxha");
    cfg = databaseConfiguration.serialize();
    cfg.field("writeQuorum", 1);
    cfg.field("failureAvailableNodesLessQuorum", true);
    cfg.field("version", (Integer) cfg.field("version") + 1);
    manager.updateCachedDatabaseConfiguration("distributed-inserttxha", cfg, true, true);
    System.out.println("\nConfiguration updated.");

    // deadlock on server3
    this.server3inDeadlock = true;
    Thread.sleep(200);  // waiting for deadlock

    // inserting record r1 and checking consistency on server1 and server2
    System.out.print("Inserting record r1 and checking consistency...");
    ODocument r1onServer1 = new ODocument("Person").fields("id", "R001", "firstName", "Han", "lastName", "Solo");
    r1onServer1.save();
    Thread.sleep(200);
    ODocument r1onServer2 = retrieveRecord(getDatabaseURL(serverInstance.get(1)), "R001");

    assertEquals(r1onServer1.field("@version"), r1onServer2.field("@version"));
    assertEquals(r1onServer1.field("id"), r1onServer2.field("id"));
    assertEquals(r1onServer1.field("firstName"), r1onServer2.field("firstName"));
    assertEquals(r1onServer1.field("lastName"), r1onServer2.field("lastName"));

    System.out.println("\tDone.");

    // initial version of the record r1
    int initialVersion = r1onServer1.field("@version");

    // creating and executing two clients c1 and c2 (updating r1)
    System.out.print("Building client c1 and client c2...");
    List<Callable<Void>> clients = new LinkedList<Callable<Void>>();
    clients.add(new ClientWriter(getDatabaseURL(serverInstance.get(0)), "R001", "Luke", "Skywalker"));
    clients.add(new ClientWriter(getDatabaseURL(serverInstance.get(1)), "R001", "Darth", "Vader"));
    System.out.println("\tDone.");
    ExecutorService executor = Executors.newCachedThreadPool();
    System.out.println("Concurrent update:");
    List<Future<Void>> futures = executor.invokeAll(clients);

    try {
      for (Future f : futures) {
        f.get();
      }
      assertTrue("Concurrent update NOT detected!", false);
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(true);
      System.out.println("Concurrent update detected!");
    }
    // wait for propagation
    Thread.sleep(500);

    // end of deadlock on server3
    this.server3inDeadlock = false;
    Thread.sleep(1000);  // waiting for sync of server3

    // check consistency
    r1onServer1 = retrieveRecord(getDatabaseURL(serverInstance.get(0)), "R001");
    r1onServer2 = retrieveRecord(getDatabaseURL(serverInstance.get(1)), "R001");
    ODocument r1onServer3  = retrieveRecord(getDatabaseURL(serverInstance.get(2)), "R001");

    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer1);
    r1onServer1.reload();
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer2);
    r1onServer2.reload();
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer3);
    r1onServer3.reload();

    if( (r1onServer1.field("firstName").equals("Luke") && r1onServer1.field("lastName").equals("Skywalker")) ||
        r1onServer1.field("firstName").equals("Darth") && r1onServer1.field("lastName").equals("Vader")) {
      assertTrue("The record has been updated by a client!", true);
    }
    else {
      assertTrue("The record has not been updated by any client!", false);
    }

    System.out.printf("Checking consistency among servers...");
    assertEquals(r1onServer1.field("@version"), r1onServer2.field("@version"));
    assertEquals(r1onServer1.field("id"), r1onServer2.field("id"));
    assertEquals(r1onServer1.field("firstName"), r1onServer2.field("firstName"));
    assertEquals(r1onServer1.field("lastName"), r1onServer2.field("lastName"));

    assertEquals(r1onServer2.field("@version"), r1onServer3.field("@version"));
    assertEquals(r1onServer2.field("id"), r1onServer3.field("id"));
    assertEquals(r1onServer2.field("firstName"), r1onServer3.field("firstName"));
    assertEquals(r1onServer2.field("lastName"), r1onServer3.field("lastName"));
    System.out.println("The records are consistent in the cluster.");

    // final version of the record r1
    System.out.print("Checking versioning...");
    int finalVersion = r1onServer1.field("@version");

    assertEquals(finalVersion, initialVersion +1);
    System.out.println("\tDone.");

  }

  @Override
  protected void onServerStarted(ServerRun server) {
    super.onServerStarted(server);

    if (serverStarted++ == (2)) {
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
                            return server3inDeadlock;
                          }
                        }, // ACTION
                new Callable() {
                  @Override
                  public Object call() throws Exception {

                    banner("STARTING BACKUP SERVER " + (2));

                    OrientGraphFactory factory = new OrientGraphFactory(
                        "plocal:target/server2/databases/" + getDatabaseName());
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

                          // SIMULATE LONG BACKUP UNTIL SPECIFIED BY VARIABLE 'server3inDeadlock'
                          while (server3inDeadlock) {
                            Thread.sleep(1000);
                          }

                          return null;
                        }
                      }, null, 9, 1000000);

                    } catch (IOException e) {
                      e.printStackTrace();
                    } finally {
                      banner("COMPLETED BACKUP SERVER " + (2));
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

}

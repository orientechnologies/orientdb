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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ServerRun;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * It checks the consistency in the cluster with the following scenario: - 3 server (quorum=1) -
 * deadlock on server3 - record r1 inserted on server1 - record r1 (version x) is present in full
 * replica on all the servers - client c1 (connected to server1) updates r1 with the value v1,
 * meanwhile client c2 (connected to server2) updates r1 with value v2 - each server updates locally
 * its version of r1 (writeQuorum=1) - when server1 and server2 receive the update message for r1
 * from the other server they ignore the message because, if the 2 versions are equal, each server
 * maintains its own record. - no exception is thrown - end of deadlock on server3 - server3 accept
 * and perform only one update between the two update messages - no exception is thrown - check
 * consistency on the nodes: - CASE 1 - r1 on server1 has the values set by the client c1 - r1 on
 * server2 has the values set by the client c2 - CASE 2 - r1 and r2 have the same value (when the
 * remote update message arrives on the current server before of the local update message, e.g. due
 * to delay in the stack) - r1 on server3 has the values set by the client c1 or the values set by
 * the client c2, but not the old one - r1 has version x+1 on all the servers
 *
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 */
public class WWConflictAndNodeInDeadlockScenarioIT extends AbstractScenarioTest {

  volatile int serverStarted = 0;
  volatile boolean backupInProgress = false;
  private AtomicBoolean server3inDeadlock = new AtomicBoolean(false);
  private HashMap<String, Object> lukeFields =
      new HashMap<String, Object>() {
        {
          put("firstName", "Luke");
          put("lastName", "Skywalker");
        }
      };
  private HashMap<String, Object> darthFields =
      new HashMap<String, Object>() {
        {
          put("firstName", "Darth");
          put("lastName", "Vader");
        }
      };

  @Test
  @Ignore
  public void test() throws Exception {

    maxRetries = 10;
    init(SERVERS);
    prepare(false);

    execute();
  }

  @Override
  public void executeTest() throws Exception {

    /*
     * Test with quorum = 1
     */

    banner("Test with quorum = 1");

    ODatabaseDocument dbServer1 = getDatabase(0);
    ODatabaseDocument dbServer2 = getDatabase(1);
    ODatabaseDocument dbServer3 = getDatabase(2);

    try {
      // changing configuration: writeQuorum=1, autoDeploy=false
      System.out.print("\nChanging configuration (writeQuorum=1, autoDeploy=false)...");

      ODocument cfg = null;
      ServerRun server = serverInstance.get(2);
      System.out.println("\nConfiguration updated.");

      // deadlock on server3
      this.server3inDeadlock.set(true);
      Thread.sleep(200); // waiting for deadlock

      // inserting record r1 and checking consistency on server1 and server2
      System.out.print(
          "Inserting record r1 and on server1 and checking consistency on both server1 and server2...");
      dbServer1.activateOnCurrentThread();
      ODocument r1onServer1 =
          new ODocument("Person").fields("id", "R001", "firstName", "Han", "lastName", "Solo");
      r1onServer1.save();
      Thread.sleep(200);
      r1onServer1 =
          retrieveRecord(
              serverInstance.get(0),
              "R001"); // This was set to get(1), but shouldn't it be get(0) for Server1?
      ODocument r1onServer2 = retrieveRecord(serverInstance.get(1), "R001");

      assertEquals((Integer) r1onServer1.field("@version"), r1onServer2.field("@version"));
      assertEquals((String) r1onServer1.field("id"), r1onServer2.field("id"));
      assertEquals((String) r1onServer1.field("firstName"), r1onServer2.field("firstName"));
      assertEquals((String) r1onServer1.field("lastName"), r1onServer2.field("lastName"));

      System.out.println("\tDone.");

      // initial version of the record r1
      int initialVersion = r1onServer1.field("@version");

      // creating and executing two clients c1 and c2 (updating r1)
      System.out.print("Building client c1 and client c2...");
      List<Callable<Void>> clients = new LinkedList<Callable<Void>>();
      clients.add(new RecordUpdater(serverInstance.get(0), r1onServer1, lukeFields, false));
      clients.add(new RecordUpdater(serverInstance.get(1), r1onServer2, darthFields, false));
      System.out.println("\tDone.");
      ExecutorService executor = Executors.newCachedThreadPool();
      System.out.println("Concurrent update:");
      List<Future<Void>> futures = executor.invokeAll(clients);

      try {
        for (Future f : futures) {
          f.get();
        }
        assertTrue("Concurrent update correctly managed!", true);
      } catch (Exception e) {
        e.printStackTrace();
        fail("Concurrent update NOT correctly managed!");
        System.out.println("Exception was thrown!");
      }
      // wait for propagation
      Thread.sleep(500);

      // end of deadlock on server3 and sync
      try {
        this.server3inDeadlock.set(false);
        Thread.sleep(500); // waiting for sync of server3
      } catch (Exception e) {
        e.printStackTrace();
        fail("Exception was thrown!");
      }

      // check consistency
      r1onServer1 = retrieveRecord(serverInstance.get(0), "R001");
      r1onServer2 = retrieveRecord(serverInstance.get(1), "R001");
      ODocument r1onServer3 = retrieveRecord(serverInstance.get(2), "R001");

      dbServer1.activateOnCurrentThread();
      r1onServer1.reload();
      dbServer2.activateOnCurrentThread();
      r1onServer2.reload();
      dbServer3.activateOnCurrentThread();
      r1onServer3.reload();

      /**
       * Checking records' values - CASE 1 - r1 on server1 has the values set by the client c1 - r1
       * on server2 has the values set by the client c2 - CASE 2 - r1 and r2 have the same value
       * (case: the "remote-update-message" arrives on the current server before of the
       * "local-update-message", e.g. due to delay in the stack)
       */
      boolean case11 = false;
      boolean case12 = false;
      boolean case2 = false;

      // r1 on server1 has the values set by the client c1
      if (r1onServer1.field("firstName").equals("Luke")
          && r1onServer1.field("lastName").equals("Skywalker")) {
        case11 = true;
        System.out.println(
            "The record on server1 has been updated by the client c1 without exceptions!");
      }

      // r1 on server2 has the values set by the client c2
      if (r1onServer2.field("firstName").equals("Darth")
          && r1onServer2.field("lastName").equals("Vader")) {
        case12 = true;
        System.out.println(
            "The record on server1 has been updated by the client c2 without exceptions!");
      }

      // r1 and r2 have the same value (when the remote update message arrives on the current server
      // before of the local update
      // message, e.g. due to delay in the stack)
      if (r1onServer1.field("firstName").equals(r1onServer2.field("firstName"))
          && r1onServer1.field("lastName").equals(r1onServer2.field("lastName"))) {
        case2 = true;
        System.out.println(
            "The record on server1 has been updated by the client c2 without exceptions!");
      }

      if ((case11 && case12) || case2) {
        assertTrue("Condition for the records' values satisfied", true);
      } else {
        fail("Condition for the records' values NOT satisfied");
      }

      // r1 on server3 has the values set by the client c1 or the values set by the client c2, but
      // not the old one
      if ((r1onServer3.field("firstName").equals("Luke")
              && r1onServer3.field("lastName").equals("Skywalker"))
          || r1onServer3.field("firstName").equals("Darth")
              && r1onServer3.field("lastName").equals("Vader")) {
        assertTrue("The record on server3 has been updated by a client without exceptions!", true);
      } else {
        fail("The record on server3 has not been updated by any client!");
      }

      // r1 has version x+1 on all the servers
      System.out.printf("Checking version consistency among servers...");

      int finalVersion = r1onServer1.field("@version");
      assertEquals(finalVersion, initialVersion + 1);

      assertEquals((Integer) r1onServer1.field("@version"), r1onServer2.field("@version"));
      assertEquals((Integer) r1onServer2.field("@version"), r1onServer3.field("@version"));
      System.out.println("Done.");
    } finally {
      this.server3inDeadlock.set(false);
      dbServer1.activateOnCurrentThread();
      dbServer1.close();
      dbServer2.activateOnCurrentThread();
      dbServer2.close();
      dbServer3.activateOnCurrentThread();
      dbServer3.close();
    }
  }

  @Override
  protected void onServerStarted(ServerRun server) {
    super.onServerStarted(server);

    if (serverStarted++ == (2)) {
      // startQueueMonitorTask();
      startCountMonitorTask("Person");

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
                            return server3inDeadlock.get();
                          }
                        }, // ACTION
                        new Callable() {
                          @Override
                          public Object call() throws Exception {

                            banner("STARTING BACKUP SERVER " + (2));

                            ODatabaseDocument g = getDatabase(2);

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

                                      // SIMULATE LONG BACKUP UNTIL SPECIFIED BY VARIABLE
                                      // 'server3inDeadlock'
                                      while (server3inDeadlock.get()) {
                                        Thread.sleep(1000);
                                      }

                                      return null;
                                    }
                                  },
                                  null,
                                  9,
                                  1000000);

                            } catch (IOException e) {
                              e.printStackTrace();
                            } finally {
                              banner("COMPLETED BACKUP SERVER " + (2));
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
  public String getDatabaseName() {
    return "distributed-wwconflict-deadlock";
  }

  //
  // /*
  // * A task representing a client that updates the value of the record with a specific id.
  // */
  //
  // protected class RecordWriter implements Callable<Void> {
  //
  // private String dbServerUrl;
  // private ODocument recordToUpdate;
  // private String firstName;
  // private String lastName;
  //
  // protected RecordWriter(String dbServerUrl, ODocument recordToUpdate, String firstName, String
  // lastName) {
  // this.dbServerUrl = dbServerUrl;
  // this.recordToUpdate = recordToUpdate;
  // this.firstName = firstName;
  // this.lastName = lastName;
  // }
  //
  // @Override
  // public Void call() throws Exception {
  //
  // // open server1 db
  // ODatabaseDocumentTx dbServer = poolFactory.get(dbServerUrl, "admin", "admin").acquire();
  //
  // // updating the record
  // ODatabaseRecordThreadLocal.instance().set(dbServer);
  // this.recordToUpdate.field("firstName",this.firstName);
  // this.recordToUpdate.field("lastName",this.lastName);
  // this.recordToUpdate.save();
  //
  // return null;
  // }
  // }

}

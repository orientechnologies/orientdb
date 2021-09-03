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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.distributed.ServerRun;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * It checks the consistency in the cluster with the following scenario: - 3 server (quorum=2) - 5
 * threads write 100 records on server3, meanwhile after 1/3 of to-write records server3 fault
 * happens. - after 2/3 to-write records are inserted server3 is restarted. - check consistency on
 * all servers: - all the records destined to server3 were redirected to an other server, so we must
 * inspect consistency for all 500 records - all records on each server are consistent in the
 * cluster
 *
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 */
public class FaultDuringWritingWithOperationRedirectScenarioIT extends AbstractScenarioTest {

  protected Timer timer = new Timer(true);
  volatile boolean inserting = true;
  volatile int serverStarted = 0;
  volatile boolean backupInProgress = false;

  @Test
  @Ignore
  public void test() throws Exception {

    maxRetries = 10;
    init(SERVERS);
    prepare(false);

    // execute writes only on server3
    executeTestsOnServers = new ArrayList<ServerRun>();
    executeTestsOnServers.add(serverInstance.get(2));

    execute();
  }

  @Override
  public void executeTest() throws Exception { //  TO-CHANGE

    List<ODocument> result = null;
    ODatabaseDocument dbServer3 = getDatabase(2);
    String dbServerUrl1 = getRemoteDatabaseURL(serverInstance.get(0));

    try {

      /*
       * Test with quorum = 2
       */

      banner("Test with quorum = 2");

      // writes on server3 (remote access) while a task is monitoring the inserted records amount
      // and shutdown server
      // after 1/3 of total number of records to insert, and restarting it when 2/3 of records were
      // inserted.
      Callable shutdownAndRestartTask =
          new ShutdownAndRestartServer(serverInstance.get(2), dbServerUrl1, "net-fault");
      final ExecutorService executor = Executors.newSingleThreadExecutor();
      Future f = executor.submit(shutdownAndRestartTask);
      executeMultipleWrites(this.executeTestsOnServers, "remote");

      f.get(); // waiting for task ending

      // waiting for changes propagation
      waitForMultipleInsertsInClassPropagation(500L, "Person", 5000L);

      // preliminar check
      dbServer3.activateOnCurrentThread();
      result = dbServer3.query(new OSQLSynchQuery<OIdentifiable>("select from Person"));
      assertEquals(500, result.size());

      // check consistency on all the server:
      // all the records destined to server3 were redirected to an other server, so we must inspect
      // consistency for all 500 records
      checkWritesAboveCluster(serverInstance, executeTestsOnServers);

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    } finally {
      dbServer3.activateOnCurrentThread();
      if (!dbServer3.isClosed()) {
        dbServer3.close();
      }
    }
  }

  protected class ShutdownAndRestartServer implements Callable<Void> {

    private ServerRun server;
    private String dbServerUrl1;
    private String faultType;

    protected ShutdownAndRestartServer(ServerRun server, String dbServerUrl1, String faultType) {
      this.server = server;
      this.dbServerUrl1 = dbServerUrl1;
      this.faultType = faultType;
    }

    @Override
    public Void call() throws Exception {

      boolean reachedAmountOfInsertedRecords = false;
      long totalNumberOfRecordsToInsert = count * writerCount;

      // open server1 db
      ODatabaseDocument dbServer1 = getDatabase(server);

      try {

        while (true) {

          // check inserted record amount
          long insertedRecords = dbServer1.countClass("Person");

          if (insertedRecords > totalNumberOfRecordsToInsert / 3) {
            System.out.println("Fault on server3: " + faultType);
            simulateServerFault(server, this.faultType);
            assertFalse(server.isActive());
            break;
          }
        }

        while (true) {

          // check inserted record amount
          //        ODatabaseRecordThreadLocal.instance().set(dbServer1);
          long insertedRecords = dbServer1.countClass("Person");

          if (insertedRecords > 2 * totalNumberOfRecordsToInsert / 3) {
            server.startServer(getDistributedServerConfiguration(server));
            System.out.println("Server 3 restarted.");
            assertTrue(server.isActive());
            break;
          }
        }
      } finally {
        dbServer1.close();
      }

      return null;
    }
  }

  @Override
  protected void onAfterExecution() throws Exception {
    inserting = false;
    Assert.assertFalse(backupInProgress);
  }

  @Override
  public String getDatabaseName() {
    return "distributed-fault-simulation";
  }
}

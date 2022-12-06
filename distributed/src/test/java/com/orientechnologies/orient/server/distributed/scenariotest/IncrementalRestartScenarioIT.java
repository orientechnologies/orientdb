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

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import com.orientechnologies.orient.server.distributed.impl.ODistributedPlugin;
import com.orientechnologies.orient.setup.ServerRun;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Ignore;
import org.junit.Test;

/**
 * It checks the consistency in the cluster with the following scenario: - 3 server (quorum=2) -
 * network fault on server2 and server3 - 5 threads for each running server write 100 records:
 * writes on server2 and server3 are redirected on server1, writes on server1 don't succeed (due to
 * the quorum) - restart server2 - 5 threads for each running server write 100 records: writes
 * server3 are redirected on server1 or server2, writes on server1 and server2 succeed - check
 * consistency - restart server3 - 5 threads on server3 write 100 records - check consistency -
 * changing quorum (quorum=1) - network fault on server2 and server3 - 3 writes on server1 checking
 * they succeed - restart server2 - 5 threads for each running server write 100 records - restart
 * server3 - 5 threads on server3 write 100 records - check consistency
 *
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 */
public class IncrementalRestartScenarioIT extends AbstractScenarioTest {

  @Ignore
  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  public void executeTest() throws Exception {

    try {

      TestQuorum2 tq2 = new TestQuorum2(serverInstance); // Connection to dbServer3
      TestQuorum1 tq1 = new TestQuorum1(serverInstance); // Connection to dbServer1
      ExecutorService exec = Executors.newSingleThreadExecutor();
      Future currentFuture = null;

      /*
       * Test with quorum = 2
       */

      try {
        // currentFuture = exec.submit(tq2);
        // currentFuture.get();
      } catch (Exception e) {
        e.printStackTrace();
        fail();
      }

      /*
       * Test with quorum = 1
       */

      try {
        currentFuture = exec.submit(tq1);
        currentFuture.get();
      } catch (Exception e) {
        e.printStackTrace();
        fail();
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private class TestQuorum2 implements Callable<Void> {

    private final String databaseUrl;
    private List<ServerRun> serverInstances;
    private List<ServerRun> executeWritesOnServers;
    private int initialCount = 0;

    public TestQuorum2(List<ServerRun> serverInstances) {

      this.serverInstances = serverInstances;
      this.executeWritesOnServers = new LinkedList<ServerRun>();
      this.executeWritesOnServers.addAll(this.serverInstances);
      this.databaseUrl = getRemoteDatabaseURL(serverInstances.get(2));
    }

    @Override
    public Void call() throws Exception {

      List<ODocument> result = null;
      OrientDB orientDb = serverInstance.get(0).getServerInstance().getContext();
      final ODatabaseDocument dbServer1 = orientDb.open(getDatabaseName(), "admin", "admin");

      try {

        /*
         * Test with quorum = 2
         */

        banner("Test with quorum = 2");

        // checking distributed configuration
        ODistributedPlugin manager =
            (ODistributedPlugin) serverInstance.get(0).getServerInstance().getDistributedManager();
        OModifiableDistributedConfiguration databaseConfiguration =
            manager.getDatabaseConfiguration(getDatabaseName()).modify();
        ODocument cfg = databaseConfiguration.getDocument();
        cfg.field("writeQuorum", 2);
        cfg.field("version", (Integer) cfg.field("version") + 1);
        manager.updateCachedDatabaseConfiguration(getDatabaseName(), databaseConfiguration);
        assertEquals(2, (int) cfg.field("writeQuorum"));

        // network fault on server2
        System.out.println("Network fault on server2.\n");
        simulateServerFault(serverInstance.get(1), "net-fault");
        assertFalse(serverInstance.get(1).isActive());

        // network fault on server3
        System.out.println("Network fault on server3.\n");
        simulateServerFault(serverInstance.get(SERVERS - 1), "net-fault");
        assertFalse(serverInstance.get(2).isActive());

        // writes on server1
        dbServer1.activateOnCurrentThread();
        try {
          new ODocument("Person").fields("name", "Jay", "surname", "Miner").save();
          new ODocument("Person").fields("name", "Luke", "surname", "Skywalker").save();
          new ODocument("Person").fields("name", "Yoda", "surname", "Nothing").save();
          fail("Record inserted with server1 running and writeQuorum=2");
        } catch (Exception e) {
          e.printStackTrace();
          assertTrue(true);
        }

        // check that no records were inserted
        result = dbServer1.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from Person"));
        assertEquals(1, result.size());
        assertEquals(0, ((Number) result.get(0).field("count")).intValue());

        // restarting server2
        try {
          System.out.println("Restarting server 2...");
          serverInstance
              .get(1)
              .startServer(getDistributedServerConfiguration(serverInstance.get(1)));
          System.out.println("Server 2 restarted.");
          assertTrue(serverInstance.get(1).isActive());
        } catch (Exception e) {
          e.printStackTrace();
        }

        // writes on server1 and server2
        executeWritesOnServers.remove(2);
        executeMultipleWrites(executeWritesOnServers, "plocal");

        // check consistency on server1 and server2
        checkWritesAboveCluster(executeWritesOnServers, executeWritesOnServers);

        // restarting server3
        try {
          System.out.println("Restarting server 3...");
          serverInstance
              .get(2)
              .startServer(getDistributedServerConfiguration(serverInstance.get(2)));
          System.out.println("Server 3 restarted.");
          assertTrue(serverInstance.get(2).isActive());
        } catch (Exception e) {
          e.printStackTrace();
        }

        // writes on server3
        executeWritesOnServers.add(serverInstance.get(2));
        executeWritesOnServers.remove(serverInstance.get(0));
        executeWritesOnServers.remove(serverInstance.get(1));
        executeMultipleWrites(executeWritesOnServers, "plocal");

        // check consistency
        executeWritesOnServers.remove(serverInstance.get(2));
        executeWritesOnServers.addAll(serverInstance);
        checkWritesAboveCluster(serverInstance, executeWritesOnServers);

      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      } finally {
        if (dbServer1 != null) {
          dbServer1.activateOnCurrentThread();
          dbServer1.close();
          ODatabaseRecordThreadLocal.instance().set(null);
        }
      }

      return null;
    }
  }

  private class TestQuorum1 implements Callable<Void> {

    private final String databaseUrl1;
    private final String databaseUrl2;
    private List<ServerRun> serverInstances;
    private List<ServerRun> executeWritesOnServers;
    private int initialCount = 0;

    public TestQuorum1(List<ServerRun> serverInstances) {

      this.serverInstances = serverInstances;
      this.executeWritesOnServers = new LinkedList<ServerRun>();
      this.executeWritesOnServers.addAll(this.serverInstances);
      this.databaseUrl1 = getPlocalDatabaseURL(serverInstances.get(0));
      this.databaseUrl2 = getPlocalDatabaseURL(serverInstances.get(1));
    }

    @Override
    public Void call() throws Exception {

      List<ODocument> result = null;
      OrientDB orientDB = serverInstances.get(0).getServerInstance().getContext();
      final ODatabaseDocument dbServer1 = orientDB.open(getDatabaseName(), "admin", "admin");

      try {

        /*
         * Test with quorum = 1
         */

        banner("Test with quorum = 1");

        // checking distributed configuration
        ODistributedPlugin manager =
            (ODistributedPlugin) serverInstance.get(0).getServerInstance().getDistributedManager();
        OModifiableDistributedConfiguration databaseConfiguration =
            manager.getDatabaseConfiguration(getDatabaseName()).modify();
        ODocument cfg = databaseConfiguration.getDocument();
        cfg.field("writeQuorum", 1);
        cfg.field("version", (Integer) cfg.field("version") + 1);
        manager.updateCachedDatabaseConfiguration(getDatabaseName(), databaseConfiguration);
        assertEquals(1, (int) cfg.field("writeQuorum"));

        // network fault on server2
        System.out.println("Network fault on server2.\n");
        simulateServerFault(serverInstance.get(1), "net-fault");
        assertFalse(serverInstance.get(1).isActive());

        // network fault on server3
        System.out.println("Network fault on server3.\n");
        simulateServerFault(serverInstance.get(2), "net-fault");
        assertFalse(serverInstance.get(2).isActive());

        // writes on server1
        dbServer1.activateOnCurrentThread();
        try {
          System.out.println("Inserting 3 record on server1...");
          new ODocument("Person").fields("name", "Darth", "surname", "Vader").save();
          new ODocument("Person").fields("name", "Luke", "surname", "Skywalker").save();
          new ODocument("Person").fields("name", "Yoda", "surname", "Nothing").save();
          System.out.println("Done.");
        } catch (Exception e) {
          e.printStackTrace();
          fail("Record not inserted even though writeQuorum=1.");
        }

        // check that records were inserted
        result = dbServer1.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from Person"));
        assertEquals(1, result.size());
        assertEquals(3, ((Number) result.get(0).field("count")).intValue());

        // restarting server2
        try {
          System.out.println("Restarting server 2...");
          serverInstance
              .get(1)
              .startServer(getDistributedServerConfiguration(serverInstance.get(1)));
          System.out.println("Server 2 restarted.");
          assertTrue(serverInstance.get(1).isActive());
        } catch (Exception e) {
          e.printStackTrace();
        }

        // writes on server1 and server2
        executeWritesOnServers.remove(2);
        executeMultipleWrites(executeWritesOnServers, "plocal");

        // check consistency on server1 and server2
        checkWritesAboveCluster(executeWritesOnServers, executeWritesOnServers);

        // restarting server3
        try {
          System.out.println("Restarting server 3...");
          serverInstance
              .get(2)
              .startServer(getDistributedServerConfiguration(serverInstance.get(2)));
          System.out.println("Server 3 restarted.");
          assertTrue(serverInstance.get(2).isActive());
        } catch (Exception e) {
          e.printStackTrace();
        }

        // writes on server3
        executeWritesOnServers.clear();
        executeWritesOnServers.add(serverInstance.get(2));
        executeMultipleWrites(executeWritesOnServers, "plocal");

        // check consistency on server1, server2 and server3
        checkWritesAboveCluster(serverInstance, serverInstance);

      } catch (Exception e) {
        e.printStackTrace();
        fail(e.getMessage());
      } finally {
        if (dbServer1 != null) {
          dbServer1.activateOnCurrentThread();
          dbServer1.close();
          ODatabaseRecordThreadLocal.instance().set(null);
        }
      }

      return null;
    }
  }

  @Override
  public String getDatabaseName() {
    return "distributed-incremental-restart";
  }
}

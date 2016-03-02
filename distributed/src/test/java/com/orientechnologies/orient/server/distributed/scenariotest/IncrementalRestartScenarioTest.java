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
import com.orientechnologies.orient.server.distributed.ServerRun;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import org.junit.Ignore;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

/**
 * It checks the consistency in the cluster with the following scenario:
 * - 3 server  (quorum=2)
 * - network fault on server2 and server3
 * - 5 threads for each running server write 100 records: writes on server2 and server3 are redirected on server1, writes on server1 don't succeed
 * - restart server2
 * - 5 threads for each running server write 100 records: writes server3 are redirected on server1 or server2, writes on server1 and server2 succeed
 * - check consistency
 * - restart server3
 * - 5 threads on server3 write 100 records
 * - check consistency
 * - changing quorum (quorum=3)
 * - network fault on server2 and server3
 * - 3 writes on server1 checking they succeed
 * - restart server2
 * - 5 threads for each running server write 100 records
 * - restart server3
 * - 5 threads on server3 write 100 records
 * - check consistency
 */

public class IncrementalRestartScenarioTest extends AbstractScenarioTest {

  @Ignore
  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    super.executeWritesOnServers.addAll(super.serverInstance);
    execute();
  }

  @Override
  public void executeTest() throws Exception {

    ODatabaseDocumentTx dbServer3 = new ODatabaseDocumentTx(getPlocalDatabaseURL(serverInstance.get(SERVERS-1))).open("admin", "admin");

    try {

      TestQuorum2 tq2 = new TestQuorum2(serverInstance);     // Connection to dbServer3
      TestQuorum1 tq3 = new TestQuorum1(serverInstance);       // Connection to dbServer1
      ExecutorService exec = Executors.newSingleThreadExecutor();
      Future currentFuture = null;


      /*
       * Test with quorum = 2
       */

      try {
        currentFuture = exec.submit(tq2);
        currentFuture.get();
      } catch (Exception e) {
        e.printStackTrace();
        assertTrue(false);
      }


      /*
       * Test with quorum = 3
       */

      try {
        currentFuture = exec.submit(tq3);
        currentFuture.get();
      } catch (Exception e) {
        e.printStackTrace();
        assertTrue(false);
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
      final ODatabaseDocumentTx dbServer1 = new ODatabaseDocumentTx(getPlocalDatabaseURL(serverInstance.get(0))).open("admin", "admin");

      try {

        /*
         * Test with quorum = 2
         */

        banner("Test with quorum = 2");

        // checking distributed configuration
        OHazelcastPlugin manager = (OHazelcastPlugin) serverInstance.get(0).getServerInstance().getDistributedManager();
        ODistributedConfiguration databaseConfiguration = manager.getDatabaseConfiguration("distributed-inserttxha");
        ODocument cfg = databaseConfiguration.serialize();
        cfg.field("failureAvailableNodesLessQuorum", true);
        cfg.field("version", (Integer) cfg.field("version") + 1);
        manager.updateCachedDatabaseConfiguration("distributed-inserttxha", cfg, true, true);
        assertEquals(2, cfg.field("writeQuorum"));

        // network fault on server2
        System.out.println("Network fault on server2.\n");
        simulateServerFault(serverInstance.get(1), "net-fault");
        assertFalse(serverInstance.get(1).isActive());

        // network fault on server3
        System.out.println("Network fault on server3.\n");
        simulateServerFault(serverInstance.get(SERVERS - 1), "net-fault");
        assertFalse(serverInstance.get(2).isActive());

        // writes on server1
        ODatabaseRecordThreadLocal.INSTANCE.set(dbServer1);
        try {
          new ODocument("Person").fields("name", "Jay", "surname", "Miner").save();
          new ODocument("Person").fields("name", "Luke", "surname", "Skywalker").save();
          new ODocument("Person").fields("name", "Yoda", "surname", "Nothing").save();
          assertTrue("Record inserted with server1 running and writeQuorum=2", false);
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
          serverInstance.get(1).startServer(getDistributedServerConfiguration(serverInstance.get(1)));
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
          serverInstance.get(2).startServer(getDistributedServerConfiguration(serverInstance.get(2)));
          System.out.println("Server 2 restarted.");
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
        assertTrue(e.getMessage(), false);
      } finally {
        if(dbServer1 != null) {
          ODatabaseRecordThreadLocal.INSTANCE.set(dbServer1);
          dbServer1.close();
          ODatabaseRecordThreadLocal.INSTANCE.set(null);
        }
      }

      return null;
    }
  }

  private class TestQuorum1 implements Callable<Void>  {

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
      final ODatabaseDocumentTx dbServer1 = new ODatabaseDocumentTx(databaseUrl1).open("admin", "admin");

      try {

        /*
         * Test with quorum = 1
         */

        banner("Test with quorum = 1");

        // checking distributed configuration
        OHazelcastPlugin manager = (OHazelcastPlugin) serverInstance.get(0).getServerInstance().getDistributedManager();
        ODistributedConfiguration databaseConfiguration = manager.getDatabaseConfiguration("distributed-inserttxha");
        ODocument cfg = databaseConfiguration.serialize();
        cfg.field("writeQuorum", 1);
        cfg.field("version", (Integer) cfg.field("version") + 1);
        manager.updateCachedDatabaseConfiguration("distributed-inserttxha", cfg, true, true);
        assertEquals(1, cfg.field("writeQuorum"));

        // network fault on server2
        System.out.println("Network fault on server2.\n");
        simulateServerFault(serverInstance.get(1), "net-fault");
        assertFalse(serverInstance.get(1).isActive());

        // network fault on server3
        System.out.println("Network fault on server3.\n");
        simulateServerFault(serverInstance.get(2), "net-fault");
        assertFalse(serverInstance.get(2).isActive());

        // writes on server1
        ODatabaseRecordThreadLocal.INSTANCE.set(dbServer1);
        try {
          new ODocument("Person").fields("name", "Jay", "surname", "Miner").save();
          new ODocument("Person").fields("name", "Luke", "surname", "Skywalker").save();
          new ODocument("Person").fields("name", "Yoda", "surname", "Nothing").save();
        } catch (Exception e) {
          e.printStackTrace();
          assertTrue("Record not inserted even though writeQuorum=1.", false);
        }

        // check that records were inserted
        result = dbServer1.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from Person"));
        assertEquals(1, result.size());
        assertEquals(3, ((Number) result.get(0).field("count")).intValue());

        // restarting server2
        try {
          serverInstance.get(1).startServer(getDistributedServerConfiguration(serverInstance.get(1)));
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
          serverInstance.get(2).startServer(getDistributedServerConfiguration(serverInstance.get(2)));
          System.out.println("Server 2 restarted.");
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
        assertTrue(e.getMessage(), false);
      } finally {
        if(dbServer1 != null) {
          ODatabaseRecordThreadLocal.INSTANCE.set(dbServer1);
          dbServer1.close();
          ODatabaseRecordThreadLocal.INSTANCE.set(null);
        }
      }

      return null;
    }
  }

}

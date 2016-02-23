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
import com.orientechnologies.orient.core.sql.OCommandSQL;
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
 * - 3 server (quorum=2)
 * - network fault on server3
 * - 5 threads for each running server write 100 records
 * - writes on server1 and server2 succeeds, writes on server3 are redirected
 * - restart server3
 * - check consistency
 * - changing quorum (quorum=3)
 * - network fault on server3
 * - writes on server1 and server2 don't succeed
 * - restart server3
 * - 5 threads for each running server write 100 records
 * - check consistency
 */

public class ShutdownAndRestartNodeScenarioTest extends AbstractScenarioTest {

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
      TestQuorum3 tq3 = new TestQuorum3(serverInstance);       // Connection to dbServer1
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

  private class TestQuorum2 implements Callable<Void>  {

    private final String    databaseUrlServer3;
    private List<ServerRun> serverInstances;
    private List<ServerRun> executeWritesOnServers;
    private int initialCount = 0;

    public TestQuorum2(List<ServerRun> serverInstances) {

      this.serverInstances = serverInstances;
      this.executeWritesOnServers = new LinkedList<ServerRun>();
      this.executeWritesOnServers.addAll(this.serverInstances);
      this.databaseUrlServer3 = getRemoteDatabaseURL(serverInstances.get(2));
    }

    @Override
    public Void call() throws Exception {

      List<ODocument> result = null;
      final ODatabaseDocumentTx dbServer3 = new ODatabaseDocumentTx(databaseUrlServer3).open("admin", "admin");

      try {

        /*
         * Test with quorum = 2
         */

        banner("Test with quorum = 2");

        // network fault on server3
        System.out.println("Network fault on server3.\n");
        simulateServerFault(this.serverInstances.get(SERVERS - 1),"net-fault");
        assertFalse(serverInstance.get(2).isActive());

        // trying write on server3, writes must be served from the first available node
        try {
          ODatabaseRecordThreadLocal.INSTANCE.set(dbServer3);
          new ODocument("Person").fields("name", "Joe", "surname", "Black").save();
          this.initialCount++;
          result = dbServer3.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from Person"));
          assertEquals(1, result.size());
          assertEquals(1, ((Number) result.get(0).field("count")).intValue());
        } catch (Exception e) {
          e.printStackTrace();
          assertTrue(e.getMessage(), false);
        }

        // writes on server1 and server2
        ODatabaseRecordThreadLocal.INSTANCE.set(null);
        this.executeWritesOnServers.remove(2);
        executeMultipleWrites(this.executeWritesOnServers, "plocal");

        // restarting server3
        serverInstance.get(2).startServer(getDistributedServerConfiguration(serverInstance.get(SERVERS - 1)));
        System.out.println("Server 3 restarted.");
        assertTrue(serverInstance.get(2).isActive());

        // check consistency
        ODatabaseRecordThreadLocal.INSTANCE.set(dbServer3);
        dbServer3.getMetadata().getSchema().reload();
        result = dbServer3.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from Person"));
        assertEquals(1, result.size());
        assertEquals(1001, ((Number) result.get(0).field("count")).intValue());
        checkWritesAboveCluster(serverInstance, executeWritesOnServers);

      } catch (Exception e) {
        e.printStackTrace();
        assertTrue(e.getMessage(), false);
      } finally {
        if(dbServer3 != null) {
          ODatabaseRecordThreadLocal.INSTANCE.set(dbServer3);
          dbServer3.close();
          ODatabaseRecordThreadLocal.INSTANCE.set(null);
        }
      }

      return null;
    }
  }

  private class TestQuorum3 implements Callable<Void>  {

    private final String databaseUrl1;
    private final String databaseUrl2;
    private List<ServerRun> serverInstances;
    private List<ServerRun> executeWritesOnServers;
    private int initialCount = 0;

    public TestQuorum3(List<ServerRun> serverInstances) {

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
         * Test with quorum = 3
         */

        banner("Test with quorum = 3");

        // deleting all
        OCommandSQL sqlCommand = new OCommandSQL("delete from Person");
        dbServer1.command(sqlCommand).execute();
        result = dbServer1.query(new OSQLSynchQuery<OIdentifiable>("select from Person"));
        assertEquals(0, result.size());
        this.initialCount = 0;

        // changing configuration
        System.out.print("\nChanging quorum...");

        ODocument cfg = null;
        ServerRun server = serverInstance.get(0);
        OHazelcastPlugin manager = (OHazelcastPlugin) server.getServerInstance().getDistributedManager();
        ODistributedConfiguration databaseConfiguration = manager.getDatabaseConfiguration("distributed-inserttxha");
        cfg = databaseConfiguration.serialize();
        cfg.field("writeQuorum", 3);
        cfg.field("failureAvailableNodesLessQuorum", true);
        cfg.field("version", (Integer) cfg.field("version") + 1);
        manager.updateCachedDatabaseConfiguration("distributed-inserttxha", cfg, true, true);

        System.out.println("\nConfiguration updated.");

        // network fault on server3
        System.out.println("Network fault on server3.\n");
        simulateServerFault(this.serverInstances.get(SERVERS - 1),"net-fault");
        assertFalse(serverInstance.get(2).isActive());

        // single write
        System.out.print("Insert operation in the database...");
        ODatabaseRecordThreadLocal.INSTANCE.set(dbServer1);
        try {
          new ODocument("Person").fields("id", "L-001", "name", "John", "surname", "Black").save();
          assertTrue("Error: record inserted with 2 server running and writeWuorum=3.", false);
        } catch (Exception e) {
          e.printStackTrace();
          assertTrue("Record not inserted because there are 2 server running and writeWuorum=3.", true);
        }
        System.out.println("Done.\n");

        System.out.print("Checking the last record wasn't inserted in the db because the quorum was not reached...");
        result = dbServer1.query(new OSQLSynchQuery<OIdentifiable>("select from Person where id='L-001'"));
        assertEquals(0, result.size());

        final ODatabaseDocumentTx dbServer2 = new ODatabaseDocumentTx(databaseUrl2).open("admin", "admin");
        ODatabaseRecordThreadLocal.INSTANCE.set(dbServer2);
        result = dbServer2.query(new OSQLSynchQuery<OIdentifiable>("select from Person where id='L-001'"));
        assertEquals(0, result.size());

        System.out.println("Done.\n");
        ODatabaseRecordThreadLocal.INSTANCE.set(null);

        // restarting server3
        serverInstance.get(2).startServer(getDistributedServerConfiguration(serverInstance.get(SERVERS - 1)));
        System.out.println("Server 3 restarted.");
        assertTrue(serverInstance.get(2).isActive());

        // writes on server1, server2 and server3
        executeMultipleWrites(this.executeWritesOnServers, "plocal");

        // check consistency
        ODatabaseRecordThreadLocal.INSTANCE.set(dbServer1);
        dbServer1.getMetadata().getSchema().reload();
        result = dbServer1.query(new OSQLSynchQuery<OIdentifiable>("select from Person"));
        assertEquals(1500, result.size());
        checkWritesAboveCluster(serverInstance, executeWritesOnServers);
        ODatabaseRecordThreadLocal.INSTANCE.set(null);

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

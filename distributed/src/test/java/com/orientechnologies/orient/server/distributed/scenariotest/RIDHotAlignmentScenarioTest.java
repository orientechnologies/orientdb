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
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ServerRun;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.*;

/**
 * It checks the consistency in the cluster with the following scenario:
 * - 3 server (quorum=1)
 * - server3 is isolated (simulated by: shutdown + opening plocal db)
 * - insert on server3 succeeds
 * - server3 joins the cluster
 * - the first record is replicated on server1 and server2
 * - two writes on server3:
 *      - the first one fails due to rid disalignment (is it right? doesn't fail!)
 *      - the second one succeeds after the fix task (wait for the alignment before the second write)
 */

public class RIDHotAlignmentScenarioTest extends AbstractScenarioTest {

  @Ignore
  @Test
  public void test() throws Exception {

    maxRetries = 10;
    init(SERVERS);
    prepare(false);

    // execute writes only on server3
    executeWritesOnServers.add(serverInstance.get(2));

    execute();
  }

  @Override
  public void executeTest() throws Exception {

    /*
     * Test with quorum = 1
     */

    banner("Test with quorum = 1");

    // changing configuration: writeQuorum=1, autoDeploy=false, hotAlignment=true
    System.out.print("\nChanging configuration (writeQuorum=1, autoDeploy=false, hotAlignment=true)...");

    ODocument cfg = null;
    ServerRun server = serverInstance.get(2);
    OHazelcastPlugin manager = (OHazelcastPlugin) server.getServerInstance().getDistributedManager();
    ODistributedConfiguration databaseConfiguration = manager.getDatabaseConfiguration("distributed-inserttxha");
    cfg = databaseConfiguration.serialize();
    cfg.field("writeQuorum", 1);
    cfg.field("failureAvailableNodesLessQuorum", true);
    cfg.field("autoDeploy", true);
    cfg.field("hotAlignment", true);
    cfg.field("version", (Integer) cfg.field("version") + 1);
    manager.updateCachedDatabaseConfiguration("distributed-inserttxha", cfg, true, true);
    System.out.println("\nConfiguration updated.");

    // creating class "Hero"
    ODatabaseDocumentTx dbServer1 = poolFactory.get(getPlocalDatabaseURL(serverInstance.get(0)), "admin", "admin").acquire();
    ODatabaseDocumentTx dbServer2 = poolFactory.get(getPlocalDatabaseURL(serverInstance.get(1)), "admin", "admin").acquire();
    ODatabaseDocumentTx dbServer3 = poolFactory.get(getPlocalDatabaseURL(serverInstance.get(2)), "admin", "admin").acquire();
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer1);
    dbServer1.getMetadata().getSchema().createClass("Hero");

    // checking the cluster "Hero" is present on each server
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer1);
    HashSet<String> clustersOnServer1 = (HashSet<String>) dbServer1.getClusterNames();
    assertTrue(clustersOnServer1.contains("hero"));
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer2);
    HashSet<String> clustersOnServer2 = (HashSet<String>) dbServer2.getClusterNames();
    assertTrue(clustersOnServer2.contains("hero"));
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer3);
    HashSet<String> clustersOnServer3 = (HashSet<String>) dbServer3.getClusterNames();
    assertTrue(clustersOnServer3.contains("hero"));

    // isolating server3
    System.out.println("Network fault on server3.\n");
    simulateServerFault(serverInstance.get(2), "net-fault");
    assertFalse(serverInstance.get(2).isActive());

    // first insert with server3 isolated from the cluster
    banner("First insert on server3 (isolated from the the cluster)");
    ODocument firstInsert = null;
    ORID rid1 = null;
    try {
      dbServer3 = new ODatabaseDocumentTx(getPlocalDatabaseURL(serverInstance.get(2))).open("admin","admin");
      firstInsert = new ODocument("Hero").fields("id", "R001", "firstName", "Luke", "lastName", "Skywalker").save();
      System.out.println("First insert: " + firstInsert.getRecord().toString());
      rid1 = firstInsert.getRecord().getIdentity();
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }

    // the record was inserted on server3
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer3);
    long recordCount = dbServer3.countClass("Hero");
    assertEquals(1, recordCount);

    // checking the record was not inserted on server1 and server2 (checking the record amount)
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer1);
    recordCount = dbServer1.countClass("Hero");
    assertEquals(0, recordCount);
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer2);
    recordCount = dbServer2.countClass("Hero");
    assertEquals(0, recordCount);

    // server3 joins the cluster
    try {
      serverInstance.get(2).startServer(getDistributedServerConfiguration(server));
    } catch (Exception e) {
      assertTrue(false);
    }

    Thread.sleep(1000);

    // the first record was replicated on server1 and server2
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer1);
    recordCount = dbServer1.countClass("Hero");
    assertEquals(1, recordCount);
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer2);
    recordCount = dbServer2.countClass("Hero");
    assertEquals(1, recordCount);

    // second insert with server3 joining the cluster that must fail
    banner("Second insert on server1 (server3 joining the cluster)");
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer1);
    ODocument secondInsert = null;
    ORID rid2 = null;
    try {
      secondInsert = new ODocument("Hero").fields("id", "R002", "firstName", "Han", "lastName", "Solo").save();
      System.out.println("Second insert: " + secondInsert.getRecord().toString());
      rid2 = secondInsert.getRecord().getIdentity();
    } catch (Exception e) {
      System.out.println("Insert failed.");
      e.printStackTrace();
    }

    // the record wasn't inserted
    recordCount = dbServer1.countClass("Hero");
    assertEquals(1, recordCount);
    System.out.println("The second record was inserted.");

    // waiting for the hotAlignment
    Thread.sleep(500);

    // third insert with server3 joining the cluster that must succeed
    banner("Third insert on server1 (server3 joining the cluster)");
    ODocument thirdInsert = null;
    ORID rid3 = null;
    try {
      thirdInsert = new ODocument("Hero").fields("id", "R002", "firstName", "Han", "lastName", "Solo").save();
      System.out.println("Third insert: " + thirdInsert.getRecord().toString());
      rid3 = thirdInsert.getRecord().getIdentity();
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(true);
    }

    // the record was inserted
    recordCount = dbServer1.countClass("Hero");
    assertEquals(2, recordCount);
    System.out.println("The third record was inserted.");

    // check consistency above the cluster
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer1);
    List<ODocument> result1 = dbServer1.query(new OSQLSynchQuery<ODocument>("select from Hero"));
    recordCount = dbServer1.countClass("Hero");
    assertEquals(2, recordCount);
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer2);
    List<ODocument> result2 = dbServer2.query(new OSQLSynchQuery<ODocument>("select from Hero"));
    recordCount = dbServer2.countClass("Hero");
    assertEquals(2, recordCount);
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer3);
    List<ODocument> result3 = dbServer3.query(new OSQLSynchQuery<ODocument>("select from Hero"));
    recordCount = dbServer3.countClass("Hero");
    assertEquals(2, recordCount);

    ODocument firstInsertServer1 = retrieveRecord(getPlocalDatabaseURL(serverInstance.get(0)), "R001");
    ODocument firstInsertServer2 = retrieveRecord(getPlocalDatabaseURL(serverInstance.get(1)), "R001");
    ODocument firstInsertServer3 = retrieveRecord(getPlocalDatabaseURL(serverInstance.get(2)), "R001");

    assertEquals(firstInsertServer1.field("firstName"), firstInsertServer2.field("firstName"));
    assertEquals(firstInsertServer1.field("lastName"), firstInsertServer2.field("lastName"));
    assertEquals(firstInsertServer2.field("firstName"), firstInsertServer3.field("firstName"));
    assertEquals(firstInsertServer2.field("lastName"), firstInsertServer3.field("lastName"));

    ODocument secondInsertServer1 = retrieveRecord(getPlocalDatabaseURL(serverInstance.get(0)), "R002");
    ODocument secondInsertServer2 = retrieveRecord(getPlocalDatabaseURL(serverInstance.get(1)), "R002");
    ODocument secondInsertServer3 = retrieveRecord(getPlocalDatabaseURL(serverInstance.get(2)), "R002");

    assertEquals(secondInsertServer1.field("firstName"), secondInsertServer2.field("firstName"));
    assertEquals(secondInsertServer1.field("lastName"), secondInsertServer2.field("lastName"));
    assertEquals(secondInsertServer2.field("firstName"), secondInsertServer3.field("firstName"));
    assertEquals(secondInsertServer2.field("lastName"), secondInsertServer3.field("lastName"));

  }

  @Override
  protected ODocument retrieveRecord(String dbUrl, String uniqueId) {
    ODatabaseDocumentTx dbServer = poolFactory.get(dbUrl, "admin", "admin").acquire();
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer);
    List<ODocument> result = dbServer.query(new OSQLSynchQuery<ODocument>("select from Hero where id = '" + uniqueId + "'"));
    if (result.size() == 0)
      assertTrue("No record found with id = '" + uniqueId + "'!", false);
    else if (result.size() > 1)
      assertTrue(result.size() + " records found with id = '" + uniqueId + "'!", false);
    ODatabaseRecordThreadLocal.INSTANCE.set(null);
    return result.get(0);
  }
}

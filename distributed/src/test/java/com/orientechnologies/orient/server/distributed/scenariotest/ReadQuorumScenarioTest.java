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
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * It checks the consistency in the cluster with the following scenario:
 * - 3 server (readQuorum=2)
 * - record r1 (version x) is present in full replica on all the servers
 * - server3 is isolated (simulated by: shutdown + opening plocal db)
 * - update of r1 on server3 succeeds, so we have r1* on server3
 * - server3 joins the cluster (restart)
 * - read on server3:
 *      - r1* on server3 is not consistent with r1 on server1 and server2 (different value and version)
 *      - must return r1 and fix r1*.
 */

public class ReadQuorumScenarioTest  extends AbstractScenarioTest {

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
     * Test with readQuorum = 2
     */

    banner("Test with readQuorum = 2");

    ODatabaseDocumentTx dbServer1 = poolFactory.get(getDatabaseURL(serverInstance.get(0)), "admin", "admin").acquire();

    // changing configuration: readQuorum=2, autoDeploy=false
    System.out.print("\nChanging configuration (readQuorum=2, autoDeploy=false)...");

    ODocument cfg = null;
    ServerRun server = serverInstance.get(2);
    OHazelcastPlugin manager = (OHazelcastPlugin) server.getServerInstance().getDistributedManager();
    ODistributedConfiguration databaseConfiguration = manager.getDatabaseConfiguration("distributed-inserttxha");
    cfg = databaseConfiguration.serialize();
    cfg.field("readQuorum", 2);
    cfg.field("failureAvailableNodesLessQuorum", true);
    cfg.field("autoDeploy", false);
    cfg.field("version", (Integer) cfg.field("version") + 1);
    manager.updateCachedDatabaseConfiguration("distributed-inserttxha", cfg, true, true);
    System.out.println("\nConfiguration updated.");

    // inserting record r1 and checking consistency on all the servers
    System.out.print("Inserting record r1 and checking consistency...");
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer1);
    new ODocument("Person").fields("id", "R001", "firstName", "Luke", "lastName", "Skywalker").save();
    Thread.sleep(200);
    ODocument r1onServer1 = retrieveRecord(getDatabaseURL(serverInstance.get(0)), "R001");
    ODocument r1onServer2 = retrieveRecord(getDatabaseURL(serverInstance.get(1)), "R001");
    ODocument r1onServer3 = retrieveRecord(getDatabaseURL(serverInstance.get(2)), "R001");

    assertEquals(r1onServer1.field("@version"), r1onServer2.field("@version"));
    assertEquals(r1onServer1.field("id"), r1onServer2.field("id"));
    assertEquals(r1onServer1.field("firstName"), r1onServer2.field("firstName"));
    assertEquals(r1onServer1.field("lastName"), r1onServer2.field("lastName"));

    assertEquals(r1onServer2.field("@version"), r1onServer3.field("@version"));
    assertEquals(r1onServer2.field("id"), r1onServer3.field("id"));
    assertEquals(r1onServer2.field("firstName"), r1onServer3.field("firstName"));
    assertEquals(r1onServer2.field("lastName"), r1onServer3.field("lastName"));

    System.out.println("\tDone.");

    // initial version of the record r1
    int initialVersion = r1onServer1.field("@version");

    // isolating server3
    System.out.println("Network fault on server3.\n");
    simulateServerFault(serverInstance.get(2), "net-fault");
    assertFalse(serverInstance.get(2).isActive());

    // updaing r1 in r1* on server3
    banner("Updaing r1 in r1* on server3 (isolated from the the cluster)");
    ODatabaseDocumentTx dbServer3 = null;
    ODocument retrievedRecord = null;
    try {
      dbServer3 = new ODatabaseDocumentTx(getPlocalDatabaseURL(serverInstance.get(2))).open("admin","admin");
      retrievedRecord = retrieveRecord(getPlocalDatabaseURL(serverInstance.get(2)), "R001");
      retrievedRecord.field("firstName","Darth");
      retrievedRecord.field("lastName","Vader");
      retrievedRecord.save();
      System.out.println(retrievedRecord.getRecord().toString());
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }

    // restarting server3
    serverInstance.get(2).startServer(getDistributedServerConfiguration(serverInstance.get(SERVERS - 1)));
    System.out.println("Server 3 restarted.");
    assertTrue(serverInstance.get(2).isActive());

    // reading r1* on server3
    dbServer3 = poolFactory.get(getDatabaseURL(serverInstance.get(2)), "admin", "admin").acquire();
    try {
      retrievedRecord = retrieveRecord(getPlocalDatabaseURL(serverInstance.get(2)), "R001");
    } catch (Exception e) {
      e.printStackTrace();
    }

    // the retrieved record must be r1 (if fix was applied)
    assertEquals("R001", retrievedRecord.field("id"));
    assertEquals("Luke", retrievedRecord.field("firstName"));
    assertEquals("Skywalker", retrievedRecord.field("lastName"));

  }

}

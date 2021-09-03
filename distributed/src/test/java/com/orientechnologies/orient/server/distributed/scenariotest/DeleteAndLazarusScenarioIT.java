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

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.impl.ODistributedPlugin;
import com.orientechnologies.orient.setup.ServerRun;
import org.junit.Ignore;
import org.junit.Test;

/**
 * It checks the consistency in the cluster with the following scenario: - 3 servers
 * (writeQuorum=majority) - record r1 (version x) is present in full replica on all the servers -
 * server3 is isolated (simulated by: shutdown + opening plocal db) - update of r1 on server3
 * succeeds, so we have r1* on server3 - server3 joins the cluster (restart) - shutdown server1 (so
 * quorum for CRUD operation on r1 will not be reached) - delete request for r1 on server3: - quorum
 * not reached because r1* on server3 is not consistent with r1 on server2 (different values and
 * versions) - delete operation is aborted on server2 and is rolled back on server3 (resurrection) -
 * restart server1 (so quorum for CRUD operation on r1 will be reached) - check consistency: r1 is
 * still present on server1 and server2, and r1* is present on server3. - delete request for r1 on
 * server1: - quorum reached - check consistency: r1 is not present on server1 and server2, and r1*
 * is not present on server3.
 *
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 */
public class DeleteAndLazarusScenarioIT extends AbstractScenarioTest {

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
     * Test with writeQuorum = majority
     */

    banner("Test with writeQuorum = majority");

    ODatabaseDocument dbServer1 = getDatabase(0);

    // changing configuration: readQuorum=2, autoDeploy=false
    System.out.print("\nChanging configuration (autoDeploy=false)...");

    ODocument cfg = null;
    ServerRun server = serverInstance.get(2);
    ODistributedPlugin manager =
        (ODistributedPlugin) server.getServerInstance().getDistributedManager();
    ODistributedConfiguration databaseConfiguration =
        manager.getDatabaseConfiguration(getDatabaseName());
    cfg = databaseConfiguration.getDocument();

    System.out.println("\nConfiguration updated.");

    // inserting record r1 and checking consistency on all the servers
    try {
      dbServer1.activateOnCurrentThread();

      System.out.print("Inserting record r1...");
      new ODocument("Person")
          .fields("id", "R001", "firstName", "Luke", "lastName", "Skywalker")
          .save();
      System.out.println("Done.");
    } catch (Exception e) {
      e.printStackTrace();
      fail("Record r1 not inserted!.");
    } finally {
      dbServer1.close();
    }

    waitForInsertedRecordPropagation("R001");

    System.out.print("Checking consistency for record r1...");
    ODocument r1onServer1 = retrieveRecord(serverInstance.get(0), "R001");
    ODocument r1onServer2 = retrieveRecord(serverInstance.get(1), "R001");
    ODocument r1onServer3 = retrieveRecord(serverInstance.get(2), "R001");

    final ORecordId r1Rid = (ORecordId) r1onServer1.getIdentity();

    assertEquals((Integer) r1onServer1.field("@version"), (Integer) r1onServer2.field("@version"));
    assertEquals((String) r1onServer1.field("id"), (String) r1onServer2.field("id"));
    assertEquals((String) r1onServer1.field("firstName"), (String) r1onServer2.field("firstName"));
    assertEquals((String) r1onServer1.field("lastName"), (String) r1onServer2.field("lastName"));

    assertEquals((Integer) r1onServer2.field("@version"), (Integer) r1onServer3.field("@version"));
    assertEquals((String) r1onServer2.field("id"), (String) r1onServer3.field("id"));
    assertEquals((String) r1onServer2.field("firstName"), (String) r1onServer3.field("firstName"));
    assertEquals((String) r1onServer2.field("lastName"), (String) r1onServer3.field("lastName"));

    System.out.println("\tDone.");

    // initial version of the record r1
    int initialVersion = r1onServer1.field("@version");

    // isolating server3
    System.out.println("Network fault on server3.\n");
    simulateServerFault(serverInstance.get(2), "net-fault");
    assertFalse(serverInstance.get(2).isActive());

    // updating r1 in r1* on server3
    banner("Updating r1* on server3 (isolated from the the cluster)");
    ODatabaseDocument dbServer3 = null;
    try {
      r1onServer3 = retrieveRecord(serverInstance.get(2), "R001");
      dbServer3 = getDatabase(2);
      r1onServer3.field("firstName", "Darth");
      r1onServer3.field("lastName", "Vader");
      r1onServer3.save();
      System.out.println(r1onServer3.getRecord().toString());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    } finally {
      if (dbServer3 != null) dbServer3.close();
    }

    // restarting server3
    serverInstance
        .get(2)
        .startServer(getDistributedServerConfiguration(serverInstance.get(SERVERS - 1)));
    System.out.println("Server 3 restarted.");
    assertTrue(serverInstance.get(2).isActive());

    // reading r1* on server3
    dbServer3 = getDatabase(2);
    try {
      r1onServer3 = retrieveRecord(serverInstance.get(2), "R001");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      dbServer3.activateOnCurrentThread();
      dbServer3.close();
    }

    // r1 was not modified both on server1 and server2
    r1onServer1 = retrieveRecord(serverInstance.get(0), "R001");
    r1onServer2 = retrieveRecord(serverInstance.get(1), "R001");

    assertEquals(1, (int) r1onServer1.field("@version"));
    assertEquals("R001", (String) r1onServer1.field("id"));
    assertEquals("Luke", (String) r1onServer1.field("firstName"));
    assertEquals("Skywalker", (String) r1onServer1.field("lastName"));

    assertEquals((Integer) r1onServer1.field("@version"), (Integer) r1onServer2.field("@version"));
    assertEquals((String) r1onServer1.field("id"), r1onServer2.field("id"));
    assertEquals((String) r1onServer1.field("firstName"), r1onServer2.field("firstName"));
    assertEquals((String) r1onServer1.field("lastName"), r1onServer2.field("lastName"));

    // checking we have different values for r1* on server3
    assertEquals("R001", (String) r1onServer3.field("id"));
    assertEquals("Darth", (String) r1onServer3.field("firstName"));
    assertEquals("Vader", (String) r1onServer3.field("lastName"));
    assertEquals(initialVersion + 1, (int) r1onServer3.field("@version"));

    // shutdown server1
    System.out.println("Network fault on server1.\n");
    simulateServerFault(serverInstance.get(0), "net-fault");
    assertFalse(serverInstance.get(0).isActive());

    // delete request on server3 for r1*
    dbServer3 = getDatabase(2);
    try {
      dbServer3.command(new OCommandSQL("delete from Person where @rid=#27:0")).execute();
    } catch (Exception e) {
      System.out.println(e.getMessage());
    } finally {
      dbServer3.close();
    }

    // restarting server1
    serverInstance.get(0).startServer(getDistributedServerConfiguration(serverInstance.get(0)));
    System.out.println("Server 1 restarted.");
    assertTrue(serverInstance.get(0).isActive());

    // r1 is still present both on server1 and server2
    r1onServer1 = retrieveRecord(serverInstance.get(0), "R001");
    r1onServer2 = retrieveRecord(serverInstance.get(1), "R001");

    assertEquals(1, (int) r1onServer1.field("@version"));
    assertEquals("R001", (String) r1onServer1.field("id"));
    assertEquals("Luke", (String) r1onServer1.field("firstName"));
    assertEquals("Skywalker", (String) r1onServer1.field("lastName"));

    assertEquals(r1onServer1.field("@version"), r1onServer2.field("@version"));
    assertEquals(r1onServer1.field("id"), r1onServer2.field("id"));
    assertEquals(r1onServer1.field("firstName"), r1onServer2.field("firstName"));
    assertEquals(r1onServer1.field("lastName"), r1onServer2.field("lastName"));

    // r1* is still present on server3
    r1onServer3 = retrieveRecord(serverInstance.get(2), "R001");

    assertEquals(2, (int) r1onServer3.field("@version"));
    assertEquals("R001", (String) r1onServer3.field("id"));
    assertEquals("Darth", (String) r1onServer3.field("firstName"));
    assertEquals("Vader", (String) r1onServer3.field("lastName"));

    // delete request on server1 for r1
    dbServer1 = getDatabase(0);
    try {
      Integer result = dbServer1.command(new OCommandSQL("delete from " + r1Rid)).execute();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      dbServer1.close();
    }

    // r1 is no more present neither on server1, server2 nor server3
    r1onServer1 =
        retrieveRecord(
            serverInstance.get(0),
            "R001",
            true,
            new OCallable<ODocument, ODocument>() {
              @Override
              public ODocument call(ODocument doc) {
                assertEquals(MISSING_DOCUMENT, doc);
                return null;
              }
            });
    r1onServer2 =
        retrieveRecord(
            serverInstance.get(1),
            "R001",
            true,
            new OCallable<ODocument, ODocument>() {
              @Override
              public ODocument call(ODocument doc) {
                assertEquals(MISSING_DOCUMENT, doc);
                return null;
              }
            });
    r1onServer3 =
        retrieveRecord(
            serverInstance.get(2),
            "R001",
            true,
            new OCallable<ODocument, ODocument>() {
              @Override
              public ODocument call(ODocument doc) {
                assertEquals(MISSING_DOCUMENT, doc);
                return null;
              }
            });
  }

  @Override
  public String getDatabaseName() {
    return "distributed-lazarus";
  }
}

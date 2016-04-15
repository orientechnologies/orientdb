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
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ServerRun;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * It checks the consistency in the cluster with the following scenario:
 * - 3 server (quorum=1)
 * - server3 is isolated (simulated by shutdown)
 * - 5 threads on both server1 and server2 write 100 records
 * - server3 joins the cluster
 * - server3 receive the delta from the cluster
 * - check consistency
 */

public class IsolatedNodeRejoinScenarioTest extends AbstractScenarioTest {

//  @Ignore
  @Test
  public void test() throws Exception {

    maxRetries = 10;
    init(SERVERS);
    prepare(false);

    // execute writes only on server1 and server2
    executeWritesOnServers.add(serverInstance.get(0));
    executeWritesOnServers.add(serverInstance.get(1));

    execute();
  }

  @Override
  public void executeTest() throws Exception {

    /*
     * Test with quorum = 1
     */

    banner("Test with quorum = 1");

    // changing configuration: writeQuorum=1, autoDeploy=false
    System.out.print("\nChanging configuration (writeQuorum=1, autoDeploy=false)...");

    ODocument cfg = null;
    ServerRun server = serverInstance.get(2);
    OHazelcastPlugin manager = (OHazelcastPlugin) server.getServerInstance().getDistributedManager();
    ODistributedConfiguration databaseConfiguration = manager.getDatabaseConfiguration(getDatabaseName());
    cfg = databaseConfiguration.serialize();
    cfg.field("writeQuorum", 1);
    cfg.field("autoDeploy", true);
    cfg.field("version", (Integer) cfg.field("version") + 1);
    manager.updateCachedDatabaseConfiguration(getDatabaseName(), cfg, true, true);
    System.out.println("\nConfiguration updated.");

    // isolating server3
    System.out.println("Network fault on server3.\n");
    simulateServerFault(serverInstance.get(2), "net-fault");
    assertFalse(serverInstance.get(2).isActive());

    // execute writes on server1 and server2
    executeMultipleWrites(super.executeWritesOnServers, "plocal");

    // server3 joins the cluster
    try {
      serverInstance.get(2).startServer(getDistributedServerConfiguration(server));
    } catch (Exception e) {
      fail();
    }

    // wait for sync
    Thread.sleep(1000);

    // check consistency
    super.checkWritesAboveCluster(serverInstance, executeWritesOnServers);

  }

  @Override
  protected ODocument retrieveRecord(String dbUrl, String uniqueId) {
    ODatabaseDocumentTx dbServer = poolFactory.get(dbUrl, "admin", "admin").acquire();
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer);
    List<ODocument> result = dbServer.query(new OSQLSynchQuery<ODocument>("select from Hero where id = '" + uniqueId + "'"));
    if (result.size() == 0)
      fail("No record found with id = '" + uniqueId + "'!");
    else if (result.size() > 1)
      fail(result.size() + " records found with id = '" + uniqueId + "'!");
    ODatabaseRecordThreadLocal.INSTANCE.set(null);
    return result.get(0);
  }

  @Override
  public String getDatabaseName() {
    return "distributed-rid-hotalignment";
  }
}

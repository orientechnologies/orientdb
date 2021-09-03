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

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import java.util.LinkedList;
import java.util.List;
import org.junit.Ignore;
import org.junit.Test;

/**
 * It checks the consistency in the cluster with the following scenario: - 3 server (europe, usa,
 * asia) - 3 shards, one for each server (client_europe, client_usa, client_asia) - writes on each
 * node (5 threads for each running server write 100 records) - check availability no-replica (you
 * can retry records of all the shards) - shutdown server3 - check availability no-replica (you can
 * retry only records in shard1 and shard2) - restart server3 - check availability no-replica (you
 * can retry records of all the shards) - this test checks also the full restore of database that
 * doesn't overwrite the client_asia cluster because owned only by asia server
 *
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 */
public class BasicShardingNoReplicaScenarioIT extends AbstractShardingScenarioTest {

  @Test
  @Ignore
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  public void executeTest() throws Exception {

    ODocument cfg = new ODocument();

    ODatabaseDocumentInternal graphNoTx = null;
    try {
      OrientDB orientDB = serverInstance.get(0).getServerInstance().getContext();
      if (!orientDB.exists(getDatabaseName())) {
        orientDB.create(getDatabaseName(), ODatabaseType.PLOCAL);
      }
      graphNoTx = (ODatabaseDocumentInternal) orientDB.open(getDatabaseName(), "admin", "admin");

      graphNoTx.command(" create class Client extends V clusters 1");
      OSchema schema = graphNoTx.getMetadata().getSchema();
      schema.reload();

      OClass clientType = schema.getClass("Client");

      OModifiableDistributedConfiguration dCfg = new OModifiableDistributedConfiguration(cfg);
      for (int i = 0; i < serverInstance.size(); ++i) {
        final String serverName =
            serverInstance.get(i).getServerInstance().getDistributedManager().getLocalNodeName();
        clientType.addCluster("client_" + serverName);

        dCfg.setServerOwner("client_" + serverName, serverName);
      }

      final OProperty prop = clientType.createProperty("name", OType.STRING);
      prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

      assertTrue(graphNoTx.getMetadata().getIndexManagerInternal().existsIndex("Client.name"));

      Thread.sleep(500);

      graphNoTx.close();

      // writes on the three clusters
      executeMultipleWritesOnShards(executeTestsOnServers, "plocal");

      // check consistency (no-replica)
      checkAvailabilityOnShardsNoReplica(serverInstance, executeTestsOnServers);

      // network fault on server3
      System.out.println("Shutdown on server3.\n");
      simulateServerFault(serverInstance.get(2), "shutdown");
      assertFalse(serverInstance.get(2).isActive());

      waitForDatabaseIsOffline(
          executeTestsOnServers
              .get(2)
              .getServerInstance()
              .getDistributedManager()
              .getLocalNodeName(),
          getDatabaseName(),
          10000);

      // check consistency (no-replica)
      executeTestsOnServers.remove(2);
      checkAvailabilityOnShardsNoReplica(executeTestsOnServers, executeTestsOnServers);

      // this query doesn't return any result
      try {
        System.out.print("Checking that records on server3 are not available in the cluster...");
        System.out.print("Checking that records on server3 are not available in the cluster...");
        graphNoTx = (ODatabaseDocumentInternal) orientDB.open(getDatabaseName(), "admin", "admin");

        graphNoTx.activateOnCurrentThread();
        final String uniqueId = "client_asia-s2-t10-v0";
        Iterable<OElement> it =
            graphNoTx
                .command(new OCommandSQL("select from Client where name = '" + uniqueId + "'"))
                .execute();
        List<OVertex> result = new LinkedList<OVertex>();
        for (OElement v : it) {
          if (v.isVertex()) {
            result.add(v.asVertex().get());
          }
        }
        assertEquals(0, result.size());
        System.out.println("Done");
        graphNoTx.close();
        ODatabaseRecordThreadLocal.instance().set(null);
      } catch (Exception e) {
        e.printStackTrace();
        fail();
      }

      // restarting server3
      serverInstance
          .get(2)
          .startServer(getDistributedServerConfiguration(serverInstance.get(SERVERS - 1)));
      System.out.println("Server 3 restarted.");
      assertTrue(serverInstance.get(2).isActive());

      waitForDatabaseIsOnline(
          0,
          serverInstance.get(2).getServerInstance().getDistributedManager().getLocalNodeName(),
          getDatabaseName(),
          10000);

      // checking server3 status by querying a record inserted on it
      try {
        System.out.print("Checking server3 status by querying a record inserted on it...");

        OrientDB orientDB1 = serverInstance.get(2).getServerInstance().getContext();
        if (!orientDB1.exists(getDatabaseName())) {
          orientDB1.create(getDatabaseName(), ODatabaseType.PLOCAL);
        }
        graphNoTx = (ODatabaseDocumentInternal) orientDB1.open(getDatabaseName(), "admin", "admin");
        graphNoTx.activateOnCurrentThread();
        final String uniqueId = "client_asia-s2-t10-v0";
        Iterable<OElement> it =
            graphNoTx
                .command(new OCommandSQL("select from Client where name = '" + uniqueId + "'"))
                .execute();
        List<OVertex> result = new LinkedList<OVertex>();
        for (OElement v : it) {
          if (v.isVertex()) {
            result.add(v.asVertex().get());
          }
        }

        assertEquals(1, result.size());
        graphNoTx.close();
        ODatabaseRecordThreadLocal.instance().set(null);
      } catch (Exception e) {
        e.printStackTrace();
        fail(e.toString());
      }

      // check consistency (no-replica)
      executeTestsOnServers.add(serverInstance.get(2));
      checkAvailabilityOnShardsNoReplica(serverInstance, executeTestsOnServers);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    } finally {
      if (!graphNoTx.isClosed()) {
        graphNoTx.activateOnCurrentThread();
        graphNoTx.close();
        ODatabaseRecordThreadLocal.instance().set(null);
      }
    }
  }

  @Override
  public String getDatabaseName() {
    return "distributed-basic-sharding";
  }
}

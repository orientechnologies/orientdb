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
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * It checks the consistency in the cluster with the following scenario: - 3 server (europe, usa, asia) - 3 shards, one for each
 * server (client_europe, client_usa, client_asia) - writes on each node (5 threads for each running server write 100 records) -
 * check availability no-replica (you can retry records of all the shards) - shutdown server3 - check availability no-replica (you
 * can retry only records in shard1 and shard2) - restart server3 - check availability no-replica (you can retry records of all the
 * shards) - this test checks also the full restore of database that doesn't overwrite the client_asia cluster because owned only by
 * asia server
 *
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 */

public class BasicShardingNoReplicaScenarioTest extends AbstractShardingScenarioTest {

  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  public void executeTest() throws Exception {

    OHazelcastPlugin manager1 = (OHazelcastPlugin) serverInstance.get(0).getServerInstance().getDistributedManager();

    final OModifiableDistributedConfiguration databaseConfiguration = manager1.getDatabaseConfiguration(this.getDatabaseName())
        .modify();
    ODocument cfg = databaseConfiguration.getDocument();

    OrientGraphFactory localFactory = new OrientGraphFactory("plocal:target/server0/databases/" + getDatabaseName());
    OrientGraphNoTx graphNoTx = null;
    try {
      graphNoTx = localFactory.getNoTx();

      final OrientVertexType clientType = graphNoTx.createVertexType("Client", 1);

      OModifiableDistributedConfiguration dCfg = new OModifiableDistributedConfiguration(cfg);
      for (int i = 0; i < serverInstance.size(); ++i) {
        final String serverName = serverInstance.get(i).getServerInstance().getDistributedManager().getLocalNodeName();
        clientType.addCluster("client_" + serverName);

        dCfg.setServerOwner("client_" + serverName, serverName);
      }
      manager1.updateCachedDatabaseConfiguration(this.getDatabaseName(), dCfg, true);

      final OrientVertexType.OrientVertexProperty prop = clientType.createProperty("name", OType.STRING);
      prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

      assertTrue(graphNoTx.getRawGraph().getMetadata().getIndexManager().existsIndex("Client.name"));

      Thread.sleep(500);

      graphNoTx.getRawGraph().close();

      // writes on the three clusters
      executeMultipleWritesOnShards(executeTestsOnServers, "plocal");

      // check consistency (no-replica)
      checkAvailabilityOnShardsNoReplica(serverInstance, executeTestsOnServers);

      // network fault on server3
      System.out.println("Shutdown on server3.\n");
      simulateServerFault(serverInstance.get(2), "shutdown");
      assertFalse(serverInstance.get(2).isActive());

      waitForDatabaseIsOffline(executeTestsOnServers.get(2).getServerInstance().getDistributedManager().getLocalNodeName(),
          getDatabaseName(), 10000);

      // check consistency (no-replica)
      executeTestsOnServers.remove(2);
      checkAvailabilityOnShardsNoReplica(executeTestsOnServers, executeTestsOnServers);

      // this query doesn't return any result
      try {
        System.out.print("Checking that records on server3 are not available in the cluster...");
        graphNoTx = localFactory.getNoTx();
        ODatabaseRecordThreadLocal.INSTANCE.set(graphNoTx.getRawGraph());
        final String uniqueId = "client_asia-s2-t10-v0";
        Iterable<Vertex> it = graphNoTx.command(new OCommandSQL("select from Client where name = '" + uniqueId + "'")).execute();
        List<OrientVertex> result = new LinkedList<OrientVertex>();
        for (Vertex v : it) {
          result.add((OrientVertex) v);
        }
        assertEquals(0, result.size());
        System.out.println("Done");
        graphNoTx.getRawGraph().close();
        ODatabaseRecordThreadLocal.INSTANCE.set(null);
      } catch (Exception e) {
        e.printStackTrace();
        fail();
      }

      // restarting server3
      serverInstance.get(2).startServer(getDistributedServerConfiguration(serverInstance.get(SERVERS - 1)));
      System.out.println("Server 3 restarted.");
      assertTrue(serverInstance.get(2).isActive());

      waitForDatabaseIsOnline(0, serverInstance.get(2).getServerInstance().getDistributedManager().getLocalNodeName(),
          getDatabaseName(), 10000);

      // checking server3 status by querying a record inserted on it
      try {
        System.out.print("Checking server3 status by querying a record inserted on it...");
        localFactory = new OrientGraphFactory("plocal:target/server2/databases/" + getDatabaseName());
        graphNoTx = localFactory.getNoTx();
        ODatabaseRecordThreadLocal.INSTANCE.set(graphNoTx.getRawGraph());
        final String uniqueId = "client_asia-s2-t10-v0";
        Iterable<Vertex> it = graphNoTx.command(new OCommandSQL("select from Client where name = '" + uniqueId + "'")).execute();
        List<OrientVertex> result = new LinkedList<OrientVertex>();
        for (Vertex v : it) {
          result.add((OrientVertex) v);
        }
        assertEquals(1, result.size());
        graphNoTx.getRawGraph().close();
        ODatabaseRecordThreadLocal.INSTANCE.set(null);
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
      if (!graphNoTx.getRawGraph().isClosed()) {
        ODatabaseRecordThreadLocal.INSTANCE.set(graphNoTx.getRawGraph());
        graphNoTx.getRawGraph().close();
        ODatabaseRecordThreadLocal.INSTANCE.set(null);
      }

    }

  }

  @Override
  public String getDatabaseName() {
    return "distributed-basic-sharding";
  }

}

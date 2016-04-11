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
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * It checks the consistency in the cluster with the following scenario: - 3 server (europe, usa, asia) - 3 shards, one for each
 * server (client_europe, client_usa, client_asia) - writes on each node (5 threads for each running server write 100 records) -
 * check consistency no-replica - shutdown server3 - check consistency no-replica (can retry only records in shard1 and shard2) -
 * restart server3 - check consistency no-replica
 */

public class BasicShardingNoReplicaScenarioTest extends AbstractShardingScenarioTest {

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

    // changing flag "failureAvailableNodesLessQuorum" because in no-rpelica case
    OHazelcastPlugin manager1 = (OHazelcastPlugin) serverInstance.get(0).getServerInstance().getDistributedManager();
    ODistributedConfiguration databaseConfiguration = manager1.getDatabaseConfiguration(this.getDatabaseName());
    ODocument cfg = databaseConfiguration.serialize();
    cfg.field("autoDeploy", false);
    cfg.field("version", (Integer) cfg.field("version") + 1);
    manager1.updateCachedDatabaseConfiguration(this.getDatabaseName(), cfg, true, true);

    OHazelcastPlugin manager2 = (OHazelcastPlugin) serverInstance.get(1).getServerInstance().getDistributedManager();
    databaseConfiguration = manager2.getDatabaseConfiguration(this.getDatabaseName());
    cfg = databaseConfiguration.serialize();
    cfg.field("autoDeploy", false);
    cfg.field("version", (Integer) cfg.field("version") + 1);
    manager2.updateCachedDatabaseConfiguration(this.getDatabaseName(), cfg, true, true);

    OHazelcastPlugin manager3 = (OHazelcastPlugin) serverInstance.get(2).getServerInstance().getDistributedManager();
    databaseConfiguration = manager3.getDatabaseConfiguration(this.getDatabaseName());
    cfg = databaseConfiguration.serialize();
    cfg.field("autoDeploy", false);
    cfg.field("version", (Integer) cfg.field("version") + 1);
    manager3.updateCachedDatabaseConfiguration(this.getDatabaseName(), cfg, true, true);

    OrientGraphFactory localFactory = new OrientGraphFactory("plocal:target/server0/databases/" + getDatabaseName());
    OrientGraphNoTx graphNoTx = null;

    try {

      graphNoTx = localFactory.getNoTx();

      // removing "person" and "cusomer" classes
      // graphNoTx.dropVertexType("Provider");
      // graphNoTx.dropVertexType("Customer");
      // graphNoTx.dropVertexType("Person");

      final OrientVertexType clientType = graphNoTx.createVertexType("Client");
      final OrientVertexType.OrientVertexProperty prop = clientType.createProperty("name", OType.STRING);
      prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

      assertTrue(graphNoTx.getRawGraph().getMetadata().getIndexManager().existsIndex("Client.name"));

      for (int i = 0; i < serverInstance.size(); ++i) {
        final String serverName = serverInstance.get(i).getServerInstance().getDistributedManager().getLocalNodeName();
        clientType.addCluster("client_" + serverName);
      }

      Thread.sleep(500);

      // checking clusters
      int[] clusterIds = clientType.getClusterIds();
      int defaultId = clientType.getDefaultClusterId();

      Map<Integer, String> id2clusterName = new HashMap<Integer, String>();
      for (int i = 0; i < clusterIds.length; i++) {
        id2clusterName.put(clusterIds[i], graphNoTx.getRawGraph().getClusterNameById(clusterIds[i]));
      }

      assertEquals(4, id2clusterName.size());
      assertEquals("client", id2clusterName.get(clusterIds[0]));
      assertEquals("client_usa", id2clusterName.get(clusterIds[1]));
      assertEquals("client_europe", id2clusterName.get(clusterIds[2]));
      assertEquals("client_asia", id2clusterName.get(clusterIds[3]));

      graphNoTx.getRawGraph().close();

      // writes on the three clusters
      executeMultipleWritesOnShards(executeWritesOnServers, "plocal");

      // check consistency (no-replica)
      checkWritesWithShardinNoReplica(serverInstance, executeWritesOnServers);

      // network fault on server3
      System.out.println("Network fault on server3.\n");
      simulateServerFault(serverInstance.get(2), "net-fault");
      assertFalse(serverInstance.get(2).isActive());

      Thread.sleep(500);

      // check consistency (no-replica)
      executeWritesOnServers.remove(2);
      checkWritesWithShardinNoReplica(executeWritesOnServers, executeWritesOnServers);

      // this query doesn't return any result
      try {
        System.out.print("Checking that records on server3 are not available in the cluster...");
        graphNoTx = localFactory.getNoTx();
        ODatabaseRecordThreadLocal.INSTANCE.set(graphNoTx.getRawGraph());
        final String uniqueId = "client_asia-s2-t10-v0";
        Iterable<Vertex> it = graphNoTx.command(new OCommandSQL("select from Client where name = '" + uniqueId + "'")).execute();
        List<OrientVertex> result = new LinkedList<OrientVertex>();
        assertEquals(0, result.size());
        System.out.println("Done");
        graphNoTx.getRawGraph().close();
        ODatabaseRecordThreadLocal.INSTANCE.set(null);
      } catch (Exception e) {
        e.printStackTrace();
        assertTrue(false);
      }

      // restarting server3
      serverInstance.get(2).startServer(getDistributedServerConfiguration(serverInstance.get(SERVERS - 1)));
      System.out.println("Server 3 restarted.");
      assertTrue(serverInstance.get(2).isActive());

      Thread.sleep(500);

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
        assertTrue(false);
      }

      // check consistency (no-replica)
      executeWritesOnServers.add(serverInstance.get(2));
      checkWritesWithShardinNoReplica(serverInstance, executeWritesOnServers);

    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(false);
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

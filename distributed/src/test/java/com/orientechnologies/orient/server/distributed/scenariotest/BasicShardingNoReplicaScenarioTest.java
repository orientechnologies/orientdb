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
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.OModifiableDistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ServerRun;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;
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

    ODatabaseDocumentTx graphNoTx = null;
    try {
      graphNoTx = new ODatabaseDocumentTx("plocal:target/server0/databases/" + getDatabaseName());
      if(graphNoTx.exists()){
        graphNoTx.open("admin", "admin");
      }else{
        graphNoTx.create();
      }

      graphNoTx.command(" create class Client clusters 1");
      OSchema schema = graphNoTx.getMetadata().getSchema();
      schema.reload();

      OClass clientType =schema.getClass("Client");

      OModifiableDistributedConfiguration dCfg = new OModifiableDistributedConfiguration(cfg);
      for (int i = 0; i < serverInstance.size(); ++i) {
        final String serverName = serverInstance.get(i).getServerInstance().getDistributedManager().getLocalNodeName();
        clientType.addCluster("client_" + serverName);

        dCfg.setServerOwner("client_" + serverName, serverName);
      }
      manager1.updateCachedDatabaseConfiguration(this.getDatabaseName(), dCfg, true);

      final OProperty prop = clientType.createProperty("name", OType.STRING);
      prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

      assertTrue(graphNoTx.getMetadata().getIndexManager().existsIndex("Client.name"));

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

      waitForDatabaseIsOffline(executeTestsOnServers.get(2).getServerInstance().getDistributedManager().getLocalNodeName(),
          getDatabaseName(), 10000);

      // check consistency (no-replica)
      executeTestsOnServers.remove(2);
      checkAvailabilityOnShardsNoReplica(executeTestsOnServers, executeTestsOnServers);

      // this query doesn't return any result
      try {
        System.out.print("Checking that records on server3 are not available in the cluster...");
        System.out.print("Checking that records on server3 are not available in the cluster...");
        graphNoTx = new ODatabaseDocumentTx("plocal:target/server0/databases/" + getDatabaseName());
        graphNoTx.open("admin", "admin");

        ODatabaseRecordThreadLocal.INSTANCE.set(graphNoTx);
        final String uniqueId = "client_asia-s2-t10-v0";
        Iterable<OElement> it = graphNoTx.command(new OCommandSQL("select from Client where name = '" + uniqueId + "'")).execute();
        List<OVertex> result = new LinkedList<OVertex>();
        for (OElement v : it) {
          if(v.isVertex()) {
            result.add(v.asVertex().get());
          }
        }
        assertEquals(0, result.size());
        System.out.println("Done");
        graphNoTx.close();
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
        
        graphNoTx = new ODatabaseDocumentTx("plocal:target/server2/databases/" + getDatabaseName());
        if(graphNoTx.exists()){
          graphNoTx.open("admin", "admin");
        }else{
          graphNoTx.create();
        }

        ODatabaseRecordThreadLocal.INSTANCE.set(graphNoTx);
        final String uniqueId = "client_asia-s2-t10-v0";
        Iterable<OElement> it = graphNoTx.command(new OCommandSQL("select from Client where name = '" + uniqueId + "'")).execute();
        List<OVertex> result = new LinkedList<OVertex>();
        for (OElement v : it) {
          if(v.isVertex()) {
            result.add(v.asVertex().get());
          }
        }

        assertEquals(1, result.size());
        graphNoTx.close();
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
      if (!graphNoTx.isClosed()) {
        ODatabaseRecordThreadLocal.INSTANCE.set(graphNoTx);
        graphNoTx.close();
        ODatabaseRecordThreadLocal.INSTANCE.set(null);
      }

    }

  }

  @Override
  public String getDatabaseName() {
    return "distributed-basic-sharding";
  }

}

/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.task.ODistributedOperationException;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 * Checks the distributed database repair feature is working.
 * 
 * @author Luca Garulli
 */
public class TestDistributedDatabaseRepair extends AbstractServerClusterTest {

  protected final static int SERVERS = 3;
  protected OrientVertex[]   vertices;

  @Test
  public void test() throws Exception {
    final long checkEvery = OGlobalConfiguration.DISTRIBUTED_CONFLICT_RESOLVER_REPAIRER_CHECK_EVERY.getValueAsLong();
    final int batchSize = OGlobalConfiguration.DISTRIBUTED_CONFLICT_RESOLVER_REPAIRER_BATCH.getValueAsInteger();

    try {

      OGlobalConfiguration.DISTRIBUTED_CONFLICT_RESOLVER_REPAIRER_CHECK_EVERY.setValue(1);
      OGlobalConfiguration.DISTRIBUTED_CONFLICT_RESOLVER_REPAIRER_BATCH.setValue(5);

      init(SERVERS);
      prepare(false);
      execute();

    } finally {
      // RESTORE DEFAULT VALUES
      OGlobalConfiguration.DISTRIBUTED_CONFLICT_RESOLVER_REPAIRER_CHECK_EVERY.setValue(checkEvery);
      OGlobalConfiguration.DISTRIBUTED_CONFLICT_RESOLVER_REPAIRER_BATCH.setValue(batchSize);
    }
  }

  @Override
  protected String getDatabaseName() {
    return "TestDistributedDatabaseRepair";
  }

  @Override
  protected void onAfterDatabaseCreation(OrientBaseGraph graphNoTx) {
  }

  @Override
  protected void executeTest() throws Exception {
    final OrientGraphFactory localFactory0 = new OrientGraphFactory("plocal:target/server0/databases/" + getDatabaseName(), false);
    final OrientGraphFactory localFactory1 = new OrientGraphFactory("plocal:target/server1/databases/" + getDatabaseName(), false);
    final OrientGraphFactory localFactory2 = new OrientGraphFactory("plocal:target/server2/databases/" + getDatabaseName(), false);
    try {
      final OrientGraphNoTx graph = localFactory0.getNoTx();
      graph.createVertexType("ProductType");
      graph.shutdown();

      testNoWinner(localFactory0, localFactory1, localFactory2);
      testWinnerIsMajority(localFactory0, localFactory1, localFactory2);
      testWinnerIsMajorityPlusVersion(localFactory0, localFactory1, localFactory2);
//      testRepairClusters(localFactory0, localFactory1, localFactory2);

    } finally {
      localFactory0.close();
      localFactory1.close();
      localFactory2.close();
    }
  }

  private void testNoWinner(OrientGraphFactory localFactory0, OrientGraphFactory localFactory1, OrientGraphFactory localFactory2)
      throws Exception {
    OrientBaseGraph graph = localFactory0.getTx();

    OrientVertex product;
    try {
      product = graph.addVertex("class:ProductType");
      product.setProperty("status", "ok");
    } finally {
      graph.shutdown();
    }

    banner("CORRUPT A RECORD IN SERVER 0");
    graph = localFactory0.getNoTx();
    try {
      final OrientVertex product2 = graph.getVertex(product.getIdentity());
      product2.getRecord().field("status", "corrupted0");
      final ODistributedResponse result = updateRemoteRecord(0, product2.getRecord(),
          new String[] { serverInstance.get(0).getServerInstance().getDistributedManager().getLocalNodeName() });

      Assert.assertFalse(result.getPayload() instanceof Throwable);
    } finally {
      graph.shutdown();
    }

    banner("CORRUPT A RECORD IN SERVER 1");
    graph = localFactory1.getNoTx();
    try {
      final OrientVertex product2 = graph.getVertex(product.getIdentity());
      product2.getRecord().field("status", "corrupted1");
      ODistributedResponse result = updateRemoteRecord(1, product2.getRecord(),
          new String[] { serverInstance.get(1).getServerInstance().getDistributedManager().getLocalNodeName() });
      Assert.assertFalse(result.getPayload() instanceof Throwable);
    } finally {
      graph.shutdown();
    }

    serverInstance.get(0).getServerInstance().getDistributedManager().getMessageService().getDatabase(getDatabaseName())
        .getDatabaseRepairer().enqueueRepairRecord((ORecordId) product.getIdentity());

    Thread.sleep(3000);

    // TEST NOTHING IS CHANGED
    graph = localFactory0.getNoTx();
    try {
      final OrientVertex product2 = graph.getVertex(product.getIdentity());
      Assert.assertEquals("corrupted0", product2.getProperty("status"));
    } finally {
      graph.shutdown();
    }

    graph = localFactory1.getNoTx();
    try {
      final OrientVertex product2 = graph.getVertex(product.getIdentity());
      Assert.assertEquals("corrupted1", product2.getProperty("status"));
    } finally {
      graph.shutdown();
    }

    graph = localFactory2.getNoTx();
    try {
      final OrientVertex product2 = graph.getVertex(product.getIdentity());
      Assert.assertEquals("ok", product2.getProperty("status"));
    } finally {
      graph.shutdown();
    }
  }

  private void testWinnerIsMajority(OrientGraphFactory localFactory0, OrientGraphFactory localFactory1,
      OrientGraphFactory localFactory2) throws Exception {
    OrientBaseGraph graph = localFactory0.getTx();

    OrientVertex product;
    try {
      product = graph.addVertex("class:ProductType");
      product.setProperty("status", "ok");
    } finally {
      graph.shutdown();
    }

    banner("CORRUPT ONLY 1 RECORD IN SERVER 0");
    graph = localFactory0.getNoTx();
    try {
      final OrientVertex product2 = graph.getVertex(product.getIdentity());
      product2.getRecord().field("status", "corrupted0");
      final ODistributedResponse result = updateRemoteRecord(0, product2.getRecord(),
          new String[] { serverInstance.get(0).getServerInstance().getDistributedManager().getLocalNodeName() });

      Assert.assertFalse(result.getPayload() instanceof Throwable);
    } finally {
      graph.shutdown();
    }

    serverInstance.get(0).getServerInstance().getDistributedManager().getMessageService().getDatabase(getDatabaseName())
        .getDatabaseRepairer().enqueueRepairRecord((ORecordId) product.getIdentity());

    Thread.sleep(3000);

    banner("EXPECTING AUTO RECOVER ON ALL NODES...");

    // TEST RECORD IS CHANGED
    graph = localFactory0.getNoTx();
    try {
      final OrientVertex product2 = graph.getVertex(product.getIdentity());
      Assert.assertEquals("ok", product2.getProperty("status"));
    } finally {
      graph.shutdown();
    }

    graph = localFactory1.getNoTx();
    try {
      final OrientVertex product2 = graph.getVertex(product.getIdentity());
      Assert.assertEquals("ok", product2.getProperty("status"));
    } finally {
      graph.shutdown();
    }

    graph = localFactory2.getNoTx();
    try {
      final OrientVertex product2 = graph.getVertex(product.getIdentity());
      Assert.assertEquals("ok", product2.getProperty("status"));
    } finally {
      graph.shutdown();
    }
  }

  private void testWinnerIsMajorityPlusVersion(OrientGraphFactory localFactory0, OrientGraphFactory localFactory1,
      OrientGraphFactory localFactory2) throws Exception {
    OrientBaseGraph graph = localFactory0.getTx();

    OrientVertex product;
    try {
      product = graph.addVertex("class:ProductType");
      product.setProperty("status", "ok");
    } finally {
      graph.shutdown();
    }

    banner("CORRUPT A RECORD IN SERVER 0");
    graph = localFactory0.getNoTx();
    try {
      final OrientVertex product2 = graph.getVertex(product.getIdentity());
      product2.getRecord().field("status", "corrupted0");
      final ODistributedResponse result = updateRemoteRecord(0, product2.getRecord(),
          new String[] { serverInstance.get(0).getServerInstance().getDistributedManager().getLocalNodeName() });

      Assert.assertFalse(result.getPayload() instanceof Throwable);
    } finally {
      graph.shutdown();
    }

    banner("CORRUPT A RECORD IN SERVER 1 WITH THE HIGHEST VERSION (=WINNER)");
    graph = localFactory1.getNoTx();
    try {
      final OrientVertex product2 = graph.getVertex(product.getIdentity());
      product2.getRecord().field("status", "thisIsTheMostRecent");
      ORecordInternal.setVersion(product2.getRecord(), ORecordVersionHelper.setRollbackMode(1000));
      ODistributedResponse result = updateRemoteRecord(1, product2.getRecord(),
          new String[] { serverInstance.get(1).getServerInstance().getDistributedManager().getLocalNodeName() });
      Assert.assertFalse(result.getPayload() instanceof Throwable);
    } finally {
      graph.shutdown();
    }

    serverInstance.get(0).getServerInstance().getDistributedManager().getMessageService().getDatabase(getDatabaseName())
        .getDatabaseRepairer().enqueueRepairRecord((ORecordId) product.getIdentity());

    Thread.sleep(3000);

    banner("EXPECTING AUTO RECOVER ON ALL NODES...");

    // TEST RECOVER IS CHANGED
    graph = localFactory0.getNoTx();
    try {
      final OrientVertex product2 = graph.getVertex(product.getIdentity());
      Assert.assertEquals("thisIsTheMostRecent", product2.getProperty("status"));
      Assert.assertEquals(1000, product2.getRecord().getVersion());
    } finally {
      graph.shutdown();
    }

    graph = localFactory1.getNoTx();
    try {
      final OrientVertex product2 = graph.getVertex(product.getIdentity());
      Assert.assertEquals("thisIsTheMostRecent", product2.getProperty("status"));
      Assert.assertEquals(1000, product2.getRecord().getVersion());
    } finally {
      graph.shutdown();
    }

    graph = localFactory2.getNoTx();
    try {
      final OrientVertex product2 = graph.getVertex(product.getIdentity());
      Assert.assertEquals("thisIsTheMostRecent", product2.getProperty("status"));
      Assert.assertEquals(1000, product2.getRecord().getVersion());
    } finally {
      graph.shutdown();
    }
  }

  /**
   * Breaks a cluster by creating new records only on certain servers
   */
  private void testRepairClusters(OrientGraphFactory localFactory0, OrientGraphFactory localFactory1,
      OrientGraphFactory localFactory2) throws Exception {

    Thread.sleep(2000);

    OrientBaseGraph graph = localFactory0.getNoTx();
    graph.createVertexType("Employee");
    graph.shutdown();

    final ODistributedConfiguration cfg = serverInstance.get(1).getServerInstance().getDistributedManager()
        .getDatabaseConfiguration(getDatabaseName());

    // FIND THE LOCAL CLUSTER
    String localCluster = null;
    final Set<String> owner = cfg
        .getClustersOwnedByServer(serverInstance.get(1).getServerInstance().getDistributedManager().getLocalNodeName());
    for (String s : owner) {
      if (s.toLowerCase().startsWith("employee")) {
        localCluster = s;
        break;
      }
    }

    graph = localFactory1.getTx();

    OrientVertex employee;
    try {
      employee = graph.addVertex("Employee", localCluster);
      employee.setProperty("status", "ok");
    } finally {
      graph.shutdown();
    }

    banner("CREATE 10 RECORDS ONLY ON local (1) SERVER and 0");
    graph = localFactory1.getTx();
    try {
      for (int i = 0; i < 10; ++i) {
        OrientVertex v = graph.addVertex("Employee", localCluster);
        v.setProperty("status", "onlyServer0and1");

        graph.getRawGraph().getStorage().getUnderlying().createRecord((ORecordId) v.getRecord().getIdentity(),
            v.getRecord().toStream(), v.getRecord().getVersion(), ODocument.RECORD_TYPE, 0, null);

        final ODistributedResponse result = createRemoteRecord(0, v.getRecord(),
            new String[] { serverInstance.get(0).getServerInstance().getDistributedManager().getLocalNodeName() });
        Assert.assertFalse(result.getPayload() instanceof Throwable);
      }
    } finally {
      graph.rollback();
    }

    banner("CREATE 10 RECORDS ONLY ON SERVER 0");
    graph = localFactory1.getTx();
    try {
      for (int i = 0; i < 10; ++i) {
        OrientVertex v = graph.addVertex("Employee", localCluster);
        v.setProperty("status", "onlyServer0and1");
        graph.getRawGraph().getStorage().getUnderlying().createRecord((ORecordId) v.getRecord().getIdentity(),
            v.getRecord().toStream(), v.getRecord().getVersion(), ODocument.RECORD_TYPE, 0, null);
      }
    } finally {
      graph.rollback();
    }

    // TRY TO CREATE A RECORD TO START THE REPAIR
    graph = localFactory1.getTx();
    try {
      OrientVertex v = graph.addVertex("Employee", localCluster);
      v.setProperty("status", "check");
      graph.commit();
    } catch (ODistributedOperationException e) {
      Assert.assertTrue(true);
    } finally {
      graph.shutdown();
    }

    Thread.sleep(5000);

    banner("CHECK RECORDS...");

    graph = localFactory0.getNoTx();
    try {
      Assert.assertEquals(21, graph.countVertices("Employee"));
    } finally {
      graph.shutdown();
    }

    graph = localFactory1.getNoTx();
    try {
      Assert.assertEquals(21, graph.countVertices("Employee"));
    } finally {
      graph.shutdown();
    }

    graph = localFactory2.getNoTx();
    try {
      Assert.assertEquals(21, graph.countVertices("Employee"));
    } finally {
      graph.shutdown();
    }
  }

}

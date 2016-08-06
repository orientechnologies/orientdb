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

import org.junit.Assert;
import org.junit.Test;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

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
      OGlobalConfiguration.DISTRIBUTED_CONFLICT_RESOLVER_REPAIRER_BATCH.setValue(0);

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

      testNoWinner(localFactory0, localFactory1, localFactory2);
      testWinnerIsMajority(localFactory0, localFactory1, localFactory2);
      testWinnerIsMajorityPlusVersion(localFactory0, localFactory1, localFactory2);
      // testRepairUnalignedRecords(localFactory0, localFactory1, localFactory2);

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
      product = graph.addVertex("class:Product-Type");
      product.setProperty("status", "ok");
    } finally {
      graph.shutdown();
    }

    banner("CORRUPT A RECORD IN SERVER 0");
    graph = localFactory0.getNoTx();
    try {
      final OrientVertex product2 = graph.getVertex(product.getIdentity());
      product2.getRecord().field("status", "corrupted0");
      final Object result = updateRemoteRecord(0, product2.getRecord(),
          new String[] { serverInstance.get(0).getServerInstance().getDistributedManager().getLocalNodeName() });

      Assert.assertFalse(result instanceof Throwable);
    } finally {
      graph.shutdown();
    }

    banner("CORRUPT A RECORD IN SERVER 1");
    graph = localFactory1.getNoTx();
    try {
      final OrientVertex product2 = graph.getVertex(product.getIdentity());
      product2.getRecord().field("status", "corrupted1");
      Object result = updateRemoteRecord(1, product2.getRecord(),
          new String[] { serverInstance.get(1).getServerInstance().getDistributedManager().getLocalNodeName() });
      Assert.assertFalse(result instanceof Throwable);
    } finally {
      graph.shutdown();
    }

    serverInstance.get(0).getServerInstance().getDistributedManager().getMessageService().getDatabase(getDatabaseName())
        .getDatabaseRapairer().repairRecord((ORecordId) product.getIdentity());

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
      product = graph.addVertex("class:Product-Type");
      product.setProperty("status", "ok");
    } finally {
      graph.shutdown();
    }

    banner("CORRUPT ONLY 1 RECORD IN SERVER 0");
    graph = localFactory0.getNoTx();
    try {
      final OrientVertex product2 = graph.getVertex(product.getIdentity());
      product2.getRecord().field("status", "corrupted0");
      final Object result = updateRemoteRecord(0, product2.getRecord(),
          new String[] { serverInstance.get(0).getServerInstance().getDistributedManager().getLocalNodeName() });

      Assert.assertFalse(result instanceof Throwable);
    } finally {
      graph.shutdown();
    }

    serverInstance.get(0).getServerInstance().getDistributedManager().getMessageService().getDatabase(getDatabaseName())
        .getDatabaseRapairer().repairRecord((ORecordId) product.getIdentity());

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
      product = graph.addVertex("class:Product-Type");
      product.setProperty("status", "ok");
    } finally {
      graph.shutdown();
    }

    banner("CORRUPT A RECORD IN SERVER 0");
    graph = localFactory0.getNoTx();
    try {
      final OrientVertex product2 = graph.getVertex(product.getIdentity());
      product2.getRecord().field("status", "corrupted0");
      final Object result = updateRemoteRecord(0, product2.getRecord(),
          new String[] { serverInstance.get(0).getServerInstance().getDistributedManager().getLocalNodeName() });

      Assert.assertFalse(result instanceof Throwable);
    } finally {
      graph.shutdown();
    }

    banner("CORRUPT A RECORD IN SERVER 1 WITH THE HIGHEST VERSION (=WINNER)");
    graph = localFactory1.getNoTx();
    try {
      final OrientVertex product2 = graph.getVertex(product.getIdentity());
      product2.getRecord().field("status", "thisIsTheMostRecent");
      ORecordInternal.setVersion(product2.getRecord(), ORecordVersionHelper.setRollbackMode(1000));
      Object result = updateRemoteRecord(1, product2.getRecord(),
          new String[] { serverInstance.get(1).getServerInstance().getDistributedManager().getLocalNodeName() });
      Assert.assertFalse(result instanceof Throwable);
    } finally {
      graph.shutdown();
    }

    serverInstance.get(0).getServerInstance().getDistributedManager().getMessageService().getDatabase(getDatabaseName())
        .getDatabaseRapairer().repairRecord((ORecordId) product.getIdentity());

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

  private void testRepairUnalignedRecords(OrientGraphFactory localFactory0, OrientGraphFactory localFactory1,
      OrientGraphFactory localFactory2) throws Exception {
    OrientBaseGraph graph = localFactory0.getTx();

    OrientVertex product;
    try {
      product = graph.addVertex("class:Product-Type");
      product.setProperty("status", "ok");
    } finally {
      graph.shutdown();
    }

    banner("CREATE A RECORD ONLY ON SERVER 0");
    final OrientVertex productOnlyOnServer0;
    graph = localFactory0.getNoTx();
    try {
      productOnlyOnServer0 = graph.addVertex("class:Product-Type");
      productOnlyOnServer0.setProperty("status", "onlyServer0");
      final Object result = createRemoteRecord(0, productOnlyOnServer0.getRecord(),
          new String[] { serverInstance.get(0).getServerInstance().getDistributedManager().getLocalNodeName() });

      Assert.assertFalse(result instanceof Throwable);
    } finally {
      graph.shutdown();
    }

    serverInstance.get(0).getServerInstance().getDistributedManager().getMessageService().getDatabase(getDatabaseName())
        .getDatabaseRapairer().repairRecord(new ORecordId(product.getIdentity().getClusterId(), -1));

    Thread.sleep(3000);

    banner("CREATE A RECORD...");
    final OrientVertex newProduct;
    graph = localFactory0.getNoTx();
    try {
      newProduct = graph.addVertex("class:Product-Type");
      newProduct.setProperty("status", "new");
      graph.commit();

    } finally {
      graph.shutdown();
    }

    Assert.assertFalse(newProduct.getIdentity().equals(productOnlyOnServer0.getIdentity()));

    banner("EXPECTING AUTO RECOVER ON ALL NODES...");

    // TEST DB ARE ALIGNED
    graph = localFactory0.getNoTx();
    try {
      final ODocument product2 = readRemoteRecord(0, (ORecordId) productOnlyOnServer0.getIdentity(),
          new String[] { serverInstance.get(0).getServerInstance().getDistributedManager().getLocalNodeName() });
      Assert.assertNotNull(product2);
      Assert.assertEquals("onlyServer0", product2.field("status"));

      final OrientVertex product3 = graph.getVertex(newProduct.getIdentity());
      Assert.assertEquals("new", product3.getProperty("status"));
    } finally {
      graph.shutdown();
    }

    graph = localFactory1.getNoTx();
    try {
      final ODocument product2 = readRemoteRecord(1, (ORecordId) productOnlyOnServer0.getIdentity(),
          new String[] { serverInstance.get(1).getServerInstance().getDistributedManager().getLocalNodeName() });
      Assert.assertNotNull(product2);
      Assert.assertEquals("onlyServer0", product2.field("status"));

      final OrientVertex product3 = graph.getVertex(newProduct.getIdentity());
      Assert.assertEquals("new", product3.getProperty("status"));
    } finally {
      graph.shutdown();
    }

    graph = localFactory2.getNoTx();
    try {
      final ODocument product2 = readRemoteRecord(2, (ORecordId) productOnlyOnServer0.getIdentity(),
          new String[] { serverInstance.get(2).getServerInstance().getDistributedManager().getLocalNodeName() });
      Assert.assertNotNull(product2);
      Assert.assertEquals("onlyServer0", product2.field("status"));

      final OrientVertex product3 = graph.getVertex(newProduct.getIdentity());
      Assert.assertEquals("new", product3.getProperty("status"));
    } finally {
      graph.shutdown();
    }
  }

}

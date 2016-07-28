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

import com.orientechnologies.orient.core.id.ORecordId;
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
    init(SERVERS);
    prepare(false);
    execute();
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

    serverInstance.get(0).getServerInstance().getDistributedManager().repairRecord(getDatabaseName(),
        (ORecordId) product.getIdentity());

    Thread.sleep(2000);

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

    serverInstance.get(0).getServerInstance().getDistributedManager().repairRecord(getDatabaseName(),
        (ORecordId) product.getIdentity());

    Thread.sleep(2000);

    banner("EXPECTING AUTO RECOVER ON ALL NODES...");

    // TEST RECOVER IS CHANGED
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
}

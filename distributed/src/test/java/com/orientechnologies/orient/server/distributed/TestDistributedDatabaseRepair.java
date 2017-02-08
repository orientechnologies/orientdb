/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.task.ODistributedOperationException;
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
  protected OVertex[] vertices;

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
  protected void onAfterDatabaseCreation(ODatabaseDocument graphNoTx) {
  }

  @Override
  protected void executeTest() throws Exception {

    final ODatabasePool localFactory0 = new ODatabasePool(serverInstance.get(0).getServerInstance().getContext(), getDatabaseName(),
        "admin", "admin");

    final ODatabasePool localFactory1 = new ODatabasePool(serverInstance.get(1).getServerInstance().getContext(), getDatabaseName(),
        "admin", "admin");

    final ODatabasePool localFactory2 = new ODatabasePool(serverInstance.get(2).getServerInstance().getContext(), getDatabaseName(),
        "admin", "admin");

    try {
      final ODatabaseDocument graph = localFactory0.acquire();
      graph.createVertexClass("ProductType");
      graph.close();

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

  private void testNoWinner(ODatabasePool localFactory0, ODatabasePool localFactory1, ODatabasePool localFactory2)
      throws Exception {
    ODatabaseDocument graph = localFactory0.acquire();

    OVertex product;
    try {
      product = graph.newVertex("ProductType");
      product.setProperty("status", "ok");
      product.save();
    } finally {
      graph.close();
    }

    banner("CORRUPT A RECORD IN SERVER 0");
    graph = localFactory0.acquire();
    try {
      final OElement product2 = graph.load(product.getIdentity());
      product2.setProperty("status", "corrupted0");
      final ODistributedResponse result = updateRemoteRecord(0, product2.getRecord(),
          new String[] { serverInstance.get(0).getServerInstance().getDistributedManager().getLocalNodeName() });

      Assert.assertFalse(result.getPayload() instanceof Throwable);
    } finally {
      graph.close();
    }

    banner("CORRUPT A RECORD IN SERVER 1");
    graph = localFactory1.acquire();
    try {
      final OElement product2 = graph.load(product.getIdentity());
      product2.setProperty("status", "corrupted1");
      ODistributedResponse result = updateRemoteRecord(1, product2.getRecord(),
          new String[] { serverInstance.get(1).getServerInstance().getDistributedManager().getLocalNodeName() });
      Assert.assertFalse(result.getPayload() instanceof Throwable);
    } finally {
      graph.close();
    }

    serverInstance.get(0).getServerInstance().getDistributedManager().getMessageService().getDatabase(getDatabaseName())
        .getDatabaseRepairer().repairRecord((ORecordId) product.getIdentity());

    Thread.sleep(3000);

    // TEST NOTHING IS CHANGED
    graph = localFactory0.acquire();
    try {
      final OElement product2 = graph.load(product.getIdentity());
      Assert.assertEquals("corrupted0", product2.getProperty("status"));
    } finally {
      graph.close();
    }

    graph = localFactory1.acquire();
    try {
      final OElement product2 = graph.load(product.getIdentity());
      Assert.assertEquals("corrupted1", product2.getProperty("status"));
    } finally {
      graph.close();
    }

    graph = localFactory2.acquire();
    try {
      final OElement product2 = graph.load(product.getIdentity());
      Assert.assertEquals("ok", product2.getProperty("status"));
    } finally {
      graph.close();
    }
  }

  private void testWinnerIsMajority(ODatabasePool localFactory0, ODatabasePool localFactory1, ODatabasePool localFactory2)
      throws Exception {
    ODatabaseDocument graph = localFactory0.acquire();
    graph.begin();

    OVertex product;
    try {
      product = graph.newVertex("ProductType");
      product.setProperty("status", "ok");
      product.save();
      graph.commit();
    } finally {
      graph.close();
    }

    banner("CORRUPT ONLY 1 RECORD IN SERVER 0");
    graph = localFactory0.acquire();
    try {
      final OElement product2 = graph.load(product.getIdentity());
      product2.setProperty("status", "corrupted0");
      product2.save();
      final ODistributedResponse result = updateRemoteRecord(0, product2.getRecord(),
          new String[] { serverInstance.get(0).getServerInstance().getDistributedManager().getLocalNodeName() });

      Assert.assertFalse(result.getPayload() instanceof Throwable);
    } finally {
      graph.close();
    }

    serverInstance.get(0).getServerInstance().getDistributedManager().getMessageService().getDatabase(getDatabaseName())
        .getDatabaseRepairer().repairRecord((ORecordId) product.getIdentity());

    Thread.sleep(3000);

    banner("EXPECTING AUTO RECOVER ON ALL NODES...");

    // TEST RECORD IS CHANGED
    graph = localFactory0.acquire();
    try {
      final OElement product2 = graph.load(product.getIdentity());
      Assert.assertEquals("ok", product2.getProperty("status"));
    } finally {
      graph.close();
    }

    graph = localFactory1.acquire();
    try {
      final OElement product2 = graph.load(product.getIdentity());
      Assert.assertEquals("ok", product2.getProperty("status"));
    } finally {
      graph.close();
    }

    graph = localFactory2.acquire();
    try {
      final OElement product2 = graph.load(product.getIdentity());
      Assert.assertEquals("ok", product2.getProperty("status"));
    } finally {
      graph.close();
    }
  }

  private void testWinnerIsMajorityPlusVersion(ODatabasePool localFactory0, ODatabasePool localFactory1,
      ODatabasePool localFactory2) throws Exception {
    ODatabaseDocument graph = localFactory0.acquire();
    graph.begin();

    OVertex product;
    try {
      product = graph.newVertex("ProductType");
      product.setProperty("status", "ok");
      product.save();
      graph.commit();
    } finally {
      graph.close();
    }

    banner("CORRUPT A RECORD IN SERVER 0");
    graph = localFactory0.acquire();
    try {
      final OElement product2 = graph.load(product.getIdentity());
      product2.setProperty("status", "corrupted0");
      product2.save();
      final ODistributedResponse result = updateRemoteRecord(0, product2.getRecord(),
          new String[] { serverInstance.get(0).getServerInstance().getDistributedManager().getLocalNodeName() });

      Assert.assertFalse(result.getPayload() instanceof Throwable);
    } finally {
      graph.close();
    }

    banner("CORRUPT A RECORD IN SERVER 1 WITH THE HIGHEST VERSION (=WINNER)");
    graph = localFactory1.acquire();
    try {
      final OElement product2 = graph.load(product.getIdentity());
      product2.setProperty("status", "thisIsTheMostRecent");
      product2.save();
      ORecordInternal.setVersion(product2.getRecord(), ORecordVersionHelper.setRollbackMode(1000));
      ODistributedResponse result = updateRemoteRecord(1, product2.getRecord(),
          new String[] { serverInstance.get(1).getServerInstance().getDistributedManager().getLocalNodeName() });
      Assert.assertFalse(result.getPayload() instanceof Throwable);
    } finally {
      graph.close();
    }

    serverInstance.get(0).getServerInstance().getDistributedManager().getMessageService().getDatabase(getDatabaseName())
        .getDatabaseRepairer().repairRecord((ORecordId) product.getIdentity());

    Thread.sleep(3000);

    banner("EXPECTING AUTO RECOVER ON ALL NODES...");

    // TEST RECOVER IS CHANGED
    graph = localFactory0.acquire();
    try {
      final OElement product2 = graph.load(product.getIdentity());
      Assert.assertEquals("thisIsTheMostRecent", product2.getProperty("status"));
      Assert.assertEquals(1000, product2.getRecord().getVersion());
    } finally {
      graph.close();
    }

    graph = localFactory1.acquire();
    try {
      final OElement product2 = graph.load(product.getIdentity());
      Assert.assertEquals("thisIsTheMostRecent", product2.getProperty("status"));
      Assert.assertEquals(1000, product2.getRecord().getVersion());
    } finally {
      graph.close();
    }

    graph = localFactory2.acquire();
    try {
      final OElement product2 = graph.load(product.getIdentity());
      Assert.assertEquals("thisIsTheMostRecent", product2.getProperty("status"));
      Assert.assertEquals(1000, product2.getRecord().getVersion());
    } finally {
      graph.close();
    }
  }

  /**
   * Breaks a cluster by creating new records only on certain servers
   */
  private void testRepairClusters(ODatabasePool localFactory0, ODatabasePool localFactory1, ODatabasePool localFactory2)
      throws Exception {

    Thread.sleep(2000);

    ODatabaseDocument graph = localFactory0.acquire();
    graph.createVertexClass("Employee");
    graph.close();

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

    graph = localFactory1.acquire();
    graph.begin();

    OVertex employee;
    try {
      employee = graph.newVertex("Employee");
      employee.setProperty("status", "ok");
      employee.save(localCluster);
      graph.commit();
    } finally {
      graph.close();
    }

    banner("CREATE 10 RECORDS ONLY ON local (1) SERVER and 0");
    graph = localFactory1.acquire();
    graph.begin();
    try {
      for (int i = 0; i < 10; ++i) {
        OVertex v = graph.newVertex("Employee");
        v.setProperty("status", "onlyServer0and1");
        v.save(localCluster);
        v.save();
        ((ODatabaseInternal) graph).getStorage().getUnderlying()
            .createRecord((ORecordId) v.getRecord().getIdentity(), v.getRecord().toStream(), v.getRecord().getVersion(),
                ODocument.RECORD_TYPE, 0, null);

        final ODistributedResponse result = createRemoteRecord(0, v.getRecord(),
            new String[] { serverInstance.get(0).getServerInstance().getDistributedManager().getLocalNodeName() });
        Assert.assertFalse(result.getPayload() instanceof Throwable);
      }
    } finally {
      graph.rollback();
    }

    banner("CREATE 10 RECORDS ONLY ON SERVER 0");
    graph = localFactory1.acquire();
    graph.begin();
    try {
      for (int i = 0; i < 10; ++i) {
        OVertex v = graph.newVertex("Employee");
        v.setProperty("status", "onlyServer0and1");
        v.save(localCluster);
        ((ODatabaseInternal) graph).getStorage().getUnderlying()
            .createRecord((ORecordId) v.getRecord().getIdentity(), v.getRecord().toStream(), v.getRecord().getVersion(),
                ODocument.RECORD_TYPE, 0, null);
      }
    } finally {
      graph.rollback();
    }

    // TRY TO CREATE A RECORD TO START THE REPAIR
    graph = localFactory1.acquire();
    graph.begin();
    try {
      OVertex v = graph.newVertex("Employee");
      v.setProperty("status", "check");
      v.save(localCluster);
      graph.commit();
    } catch (ODistributedOperationException e) {
      Assert.assertTrue(true);
    } finally {
      graph.close();
    }

    Thread.sleep(5000);

    banner("CHECK RECORDS...");

    graph = localFactory0.acquire();
    try {
      Assert.assertEquals(21, graph.getClass("Employee").count());
    } finally {
      graph.close();
    }

    graph = localFactory1.acquire();
    try {
      Assert.assertEquals(21, graph.getClass("Employee").count());
    } finally {
      graph.close();
    }

    graph = localFactory2.acquire();
    try {
      Assert.assertEquals(21, graph.getClass("Employee").count());
    } finally {
      graph.close();
    }
  }

}

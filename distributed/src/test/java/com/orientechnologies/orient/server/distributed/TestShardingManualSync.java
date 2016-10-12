package com.orientechnologies.orient.server.distributed;

import org.junit.Assert;
import org.junit.Test;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

/**
 * Tests with 2 servers the ability to resync a cluster manually.
 * 
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class TestShardingManualSync extends AbstractServerClusterTest {

  protected final static int SERVERS = 2;

  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(true);
    execute();
  }

  @Override
  protected String getDatabaseName() {
    return "sharding-manual-synch";
  }

  @Override
  protected String getDistributedServerConfiguration(final ServerRun server) {
    return "sharded-dserver-config-noautodeploy-" + server.getServerId() + ".xml";
  }

  @Override
  protected void executeTest() throws Exception {
    final OrientGraphFactory localFactoryEurope = new OrientGraphFactory("plocal:target/server0/databases/" + getDatabaseName());
    OrientGraphFactory localFactoryUsa = new OrientGraphFactory("plocal:target/server1/databases/" + getDatabaseName());

    final ORID v1Identity;

    OrientGraphNoTx graphNoTxEurope = localFactoryEurope.getNoTx();
    try {
      final OrientVertexType clientType = graphNoTxEurope.createVertexType("Client-Type");
      for (int i = 1; i < serverInstance.size(); ++i) {
        final String serverName = serverInstance.get(i).getServerInstance().getDistributedManager().getLocalNodeName();
        clientType.addCluster("client_" + serverName);
      }

      final OrientVertex v1 = graphNoTxEurope.addVertex("class:Client-Type");
      v1Identity = v1.getIdentity();
      log("Created vertex " + v1Identity + "...");

    } finally {
      graphNoTxEurope.shutdown();
    }

    OrientGraphNoTx graphNoTxUsa = localFactoryUsa.getNoTx();
    try {
      Assert.assertEquals(1, graphNoTxUsa.countVertices());
    } finally {
      graphNoTxUsa.shutdown();
      localFactoryUsa.close();
    }

    final String clusterName;

    graphNoTxEurope = localFactoryEurope.getNoTx();
    try {
      Assert.assertEquals(1, graphNoTxEurope.countVertices());

      // CHANGE THE WRITE QUORUM = 1
      final ODistributedConfiguration dCfg = serverInstance.get(0).server.getDistributedManager()
          .getDatabaseConfiguration(getDatabaseName());
      ODocument newCfg = dCfg.getDocument().field("writeQuorum", 1);
      serverInstance.get(0).server.getDistributedManager().updateCachedDatabaseConfiguration(getDatabaseName(), newCfg, true, true);

      // CREATE A NEW RECORD ON SERVER 0 BYPASSING REPLICATION
      final ODocument v2 = new ODocument("Client");
      ((ORecordId) v2.getIdentity()).clusterId = v1Identity.getClusterId();
      ((ORecordId) v2.getIdentity()).clusterPosition = v1Identity.getClusterPosition() + 1;
      final Object result = createRemoteRecord(0, v2,
          new String[] { serverInstance.get(0).getServerInstance().getDistributedManager().getLocalNodeName() });

      Assert.assertFalse(result instanceof Throwable);

      Assert.assertEquals(2, graphNoTxEurope.countVertices());

      clusterName = graphNoTxEurope.getRawGraph().getClusterNameById(v2.getIdentity().getClusterId());

      Assert.assertEquals(2, graphNoTxEurope.countVertices());
    } finally {
      graphNoTxEurope.shutdown();
    }

    // TEST SECOND VERTEX IS MISSING ON USA NODE
    localFactoryUsa = new OrientGraphFactory("plocal:target/server1/databases/" + getDatabaseName());
    graphNoTxUsa = localFactoryUsa.getNoTx();
    try {
      Assert.assertEquals(1, graphNoTxUsa.countVertices());

      log("Manually syncing cluster client-type of node USA...");
      graphNoTxUsa.command(new OCommandSQL("ha sync cluster `" + clusterName + "`")).execute();

      Assert.assertEquals(2, graphNoTxUsa.countVertices());

    } finally {
      graphNoTxUsa.shutdown();
    }

    localFactoryEurope.close();
    localFactoryUsa.close();
  }

}

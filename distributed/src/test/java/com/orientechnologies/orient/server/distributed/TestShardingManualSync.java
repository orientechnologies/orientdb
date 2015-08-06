package com.orientechnologies.orient.server.distributed;

import junit.framework.Assert;

import org.junit.Test;

import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

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
    final OrientGraphFactory localFactoryUsa = new OrientGraphFactory("plocal:target/server1/databases/" + getDatabaseName());

    OrientGraphNoTx graphNoTxEurope = localFactoryEurope.getNoTx();
    try {
      final OrientVertexType clientType = graphNoTxEurope.createVertexType("Client");
      for (int i = 1; i < serverInstance.size(); ++i) {
        final String serverName = serverInstance.get(i).getServerInstance().getDistributedManager().getLocalNodeName();
        clientType.addCluster("client_" + serverName);
      }

      final OrientVertex v1 = graphNoTxEurope.addVertex("class:Client");
    } finally {
      graphNoTxEurope.shutdown();
    }

    OrientGraphNoTx graphNoTxUsa = localFactoryUsa.getNoTx();
    try {
      Assert.assertEquals(1, graphNoTxUsa.countVertices());

      // SHUTDOWN USA SERVER
      serverInstance.get(1).shutdownServer();

    } finally {
      graphNoTxEurope.shutdown();
    }

    graphNoTxEurope = localFactoryEurope.getNoTx();
    try {
      log("Adding vertex to europe node...");

      final OrientVertex v2 = graphNoTxEurope.addVertex("class:Client");

      log("Restarting USA server...");

      // RESTART USA SERVER
      serverInstance.get(1).startServer(getDistributedServerConfiguration(serverInstance.get(1)));

      Assert.assertEquals(2, graphNoTxEurope.countVertices());

    } finally {
      graphNoTxEurope.shutdown();
    }

    // TEST SECOND VERTEX IS MISSING ON USA NODE
    graphNoTxUsa = localFactoryUsa.getNoTx();
    try {
      Assert.assertEquals(1, graphNoTxUsa.countVertices());

      log("Manually syncing cluster client of node USA...");
      graphNoTxUsa.command(new OCommandSQL("sync cluster client")).execute();

      Assert.assertEquals(2, graphNoTxUsa.countVertices());

    } finally {
      graphNoTxEurope.shutdown();
    }

    localFactoryEurope.close();
    localFactoryUsa.close();
  }

}

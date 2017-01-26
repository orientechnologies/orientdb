package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.junit.Assert;
import org.junit.Test;

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
    ODatabasePool localFactoryEurope = OrientDB.fromUrl("embedded:target/server0/databases/", OrientDBConfig.defaultConfig())
        .openPool(getDatabaseName(), "admin", "admin");
    ODatabasePool localFactoryUsa = OrientDB.fromUrl("embedded:target/server1/databases/", OrientDBConfig.defaultConfig())
        .openPool(getDatabaseName(), "admin", "admin");


    final ORID v1Identity;

    ODatabaseDocument graphNoTxEurope = localFactoryEurope.acquire();
    try {
      final OClass clientType = graphNoTxEurope.createVertexClass("Client-Type");
      for (int i = 1; i < serverInstance.size(); ++i) {
        final String serverName = serverInstance.get(i).getServerInstance().getDistributedManager().getLocalNodeName();
        clientType.addCluster("client_" + serverName);
      }

      final OVertex v1 = graphNoTxEurope.newVertex("Client-Type").save();
      v1Identity = v1.getIdentity();
      log("Created vertex " + v1Identity + "...");

    } finally {
      graphNoTxEurope.close();
    }

    ODatabaseDocument graphNoTxUsa = localFactoryUsa.acquire();
    try {
      Assert.assertEquals(1, graphNoTxUsa.getClass("V").count());
    } finally {
      graphNoTxUsa.close();
      localFactoryUsa.close();
    }

    final String clusterName;

    graphNoTxEurope = localFactoryEurope.acquire();
    try {
      Assert.assertEquals(1, graphNoTxEurope.getClass("V").count());

      // CHANGE THE WRITE QUORUM = 1
      final ODistributedConfiguration dCfg = serverInstance.get(0).server.getDistributedManager()
          .getDatabaseConfiguration(getDatabaseName());
      ODocument newCfg = dCfg.getDocument().field("writeQuorum", 1);
      serverInstance.get(0).server.getDistributedManager().updateCachedDatabaseConfiguration(getDatabaseName(), newCfg, true, true);

      // CREATE A NEW RECORD ON SERVER 0 BYPASSING REPLICATION
      final ODocument v2 = new ODocument("Client");
      ((ORecordId) v2.getIdentity()).setClusterId(v1Identity.getClusterId());
      ((ORecordId) v2.getIdentity()).setClusterPosition(v1Identity.getClusterPosition() + 1);
      final Object result = createRemoteRecord(0, v2,
          new String[] { serverInstance.get(0).getServerInstance().getDistributedManager().getLocalNodeName() });

      Assert.assertFalse(result instanceof Throwable);

      Assert.assertEquals(2, graphNoTxEurope.getClass("V").count());

      clusterName = graphNoTxEurope.getClusterNameById(v2.getIdentity().getClusterId());

      Assert.assertEquals(2, graphNoTxEurope.getClass("V").count());
    } finally {
      graphNoTxEurope.close();
    }

    // TEST SECOND VERTEX IS MISSING ON USA NODE
    localFactoryUsa = OrientDB.fromUrl("embedded:target/server1/databases/", OrientDBConfig.defaultConfig())
        .openPool(getDatabaseName(), "admin", "admin");

    graphNoTxUsa = localFactoryUsa.acquire();
    try {
      Assert.assertEquals(1, graphNoTxUsa.getClass("V").count());

      log("Manually syncing cluster client-type of node USA...");
      graphNoTxUsa.command(new OCommandSQL("ha sync cluster `" + clusterName + "`")).execute();

      Assert.assertEquals(2, graphNoTxUsa.getClass("V").count());

    } finally {
      graphNoTxUsa.close();
    }

    localFactoryEurope.close();
    localFactoryUsa.close();
  }

}

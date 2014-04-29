package com.orientechnologies.orient.server.distributed;

import junit.framework.Assert;

import org.junit.Test;

import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

public class TestSharding extends AbstractServerClusterTest {

  protected OrientVertex[] vertices;

  @Test
  public void test() throws Exception {
    init(3);
    prepare(false);
    execute();
  }

  @Override
  protected String getDatabaseName() {
    return "sharding";
  }

  @Override
  protected String getDistributedServerConfiguration(final ServerRun server) {
    return "sharded-dserver-config-" + server.getServerId() + ".xml";
  }

  @Override
  protected void executeTest() throws Exception {
    try {
      OrientGraphFactory factory = new OrientGraphFactory("plocal:target/server0/databases/" + getDatabaseName());
      OrientGraphNoTx graph = factory.getNoTx();

      try {
        final OrientVertexType clientType = graph.createVertexType("Client");

        vertices = new OrientVertex[serverInstance.size()];
        for (int i = 0; i < vertices.length; ++i) {
          clientType.addCluster("client_" + i);

          vertices[i] = graph.addVertex("class:Client,cluster:client_" + i);

          final int clId = vertices[i].getIdentity().getClusterId();

          System.out.println("Create vertex, class: " + vertices[i].getLabel() + ", cluster: " + clId);

          Assert
              .assertEquals("Error on assigning cluster client_" + i, clId, graph.getRawGraph().getClusterIdByName("client_" + i));

          vertices[i].setProperty("name", "shard_" + i);
        }
      } finally {
        graph.shutdown();
      }

      // FOR ALL THE DATABASES QUERY THE SINGLE CLUSTER TO TEST ROUTING
      for (int server = 0; server < vertices.length; ++server) {
        OrientGraphFactory f = new OrientGraphFactory("plocal:target/server" + server + "/databases/" + getDatabaseName());
        OrientGraphNoTx g = f.getNoTx();

        try {
          for (int cluster = 0; cluster < vertices.length; ++cluster) {
            final String query = "select from cluster:client_" + cluster;
            Iterable<OrientVertex> result = g.command(new OCommandSQL(query)).execute();
            Assert.assertTrue("Error on query on cluster_" + cluster + " on server " + server + ": " + query, result.iterator()
                .hasNext());

            OrientVertex v = result.iterator().next();

            Assert.assertEquals("Returned vertices name property is != shard_" + cluster + " on server " + server,
                v.getProperty("name"), "shard_" + cluster);
          }
        } finally {
          graph.shutdown();
        }
      }

      // TEST DISTRIBUTED QUERY AGAINST ALL 3 DATABASES TO TEST MAP/REDUCE
      for (int server = 0; server < vertices.length; ++server) {
        OrientGraphFactory f = new OrientGraphFactory("plocal:target/server" + server + "/databases/" + getDatabaseName());
        OrientGraphNoTx g = f.getNoTx();
        try {

          Iterable<OrientVertex> result = g.command(new OCommandSQL("select from Client")).execute();
          int count = 0;
          for (OrientVertex v : result)
            count++;

          Assert.assertEquals("Returned wrong vertices count on server " + server, count, 3);
        } finally {
          g.shutdown();
        }
      }
    } catch (Exception e) {
      // WAIT FOR TERMINATION
      Thread.sleep(10000);
      throw e;
    }
  }
}

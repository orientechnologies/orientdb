package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import junit.framework.Assert;
import org.junit.Test;

public class TestSharding extends AbstractServerClusterTest {

  protected OrientVertex[] vertices;

  @Test
  //@Ignore
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

          Assert
              .assertEquals("Error on assigning cluster client_" + i, clId, graph.getRawGraph().getClusterIdByName("client_" + i));

          vertices[i].setProperty("name", "shard_" + i);
          vertices[i].setProperty("amount", i * 10000);

          System.out.println("Create vertex, class: " + vertices[i].getLabel() + ", cluster: " + clId + " -> "
              + vertices[i].getRecord());
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

            Assert.assertEquals("Returned vertices name property is != shard_" + cluster + " on server " + server, "shard_"
                + cluster, v.getProperty("name"));
          }
        } finally {
          graph.shutdown();
        }
      }

      // TEST DISTRIBUTED QUERY + AGGREGATION + SUB_QUERY AGAINST ALL 3 DATABASES TO TEST MAP/REDUCE
      for (int server = 0; server < vertices.length; ++server) {
        OrientGraphFactory f = new OrientGraphFactory("plocal:target/server" + 0 + "/databases/" + getDatabaseName());
        OrientGraphNoTx g = f.getNoTx();
        try {
          // MISC QUERIES
          Iterable<OrientVertex> result = g.command(new OCommandSQL("select sum(amount) from ( select from Client )")).execute();

          int count = 0;
          for (OrientVertex v : result) {
            System.out.println("select sum(amount) from ( select from Client ) -> " + v.getRecord());
            count++;
          }

          Assert.assertEquals("Returned wrong vertices count on server " + server, 1, count);

        } finally {
          g.shutdown();
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

          Assert.assertEquals("Returned wrong vertices count on server " + server, 3, count);
        } finally {
          g.shutdown();
        }
      }

      // TEST DISTRIBUTED QUERY AGAINST ALL 3 DATABASES TO TEST AGGREGATION
      for (int server = 0; server < vertices.length; ++server) {
        OrientGraphFactory f = new OrientGraphFactory("plocal:target/server" + server + "/databases/" + getDatabaseName());
        OrientGraphNoTx g = f.getNoTx();
        try {

          Iterable<OrientVertex> result = g.command(new OCommandSQL("select max(amount), avg(amount), sum(amount) from Client"))
              .execute();

          int count = 0;
          for (OrientVertex v : result) {
            System.out.println("select max(amount), avg(amount), sum(amount) from Client -> " + v.getRecord());
            count++;
          }

          Assert.assertEquals("Returned wrong vertices count on server " + server, 1, count);
        } finally {
          g.shutdown();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();

      // WAIT FOR TERMINATION
      Thread.sleep(10000);
      throw e;
    }
  }
}

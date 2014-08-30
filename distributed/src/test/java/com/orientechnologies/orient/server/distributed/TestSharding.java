package com.orientechnologies.orient.server.distributed;

import junit.framework.Assert;

import org.junit.Test;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

public class TestSharding extends AbstractServerClusterTest {

  protected OrientVertex[] clients;

  @Test
  // @Ignore
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
      OrientGraphNoTx graphNoTx = factory.getNoTx();

      try {
        final OrientVertexType clientType = graphNoTx.createVertexType("Client");
        for (int i = 0; i < serverInstance.size(); ++i)
          clientType.addCluster("client_" + i);

        graphNoTx.createVertexType("Product");
        graphNoTx.createVertexType("Hobby");

        graphNoTx.createEdgeType("Knows");
        graphNoTx.createEdgeType("Buy");
        graphNoTx.createEdgeType("Loves");
      } finally {
        graphNoTx.shutdown();
      }

      final OrientVertex product;
      final OrientVertex fishing;

      OrientBaseGraph graph = factory.getTx();
      try {
        product = graph.addVertex("class:Product");

        fishing = graph.addVertex("class:Hobby");
        fishing.setProperty("name", "Fishing");

        clients = new OrientVertex[serverInstance.size()];
        for (int i = 0; i < clients.length; ++i) {
          clients[i] = graph.addVertex("class:Client,cluster:client_" + i);

          final int clId = clients[i].getIdentity().getClusterId();

          Assert
              .assertEquals("Error on assigning cluster client_" + i, clId, graph.getRawGraph().getClusterIdByName("client_" + i));

          clients[i].setProperty("name", "shard_" + i);
          clients[i].setProperty("amount", i * 10000);

          System.out.println("Create vertex, class: " + clients[i].getLabel() + ", cluster: " + clId + " -> "
              + clients[i].getRecord());

          if (i > 1)
            // CREATE A LIGHT-WEIGHT EDGE
            clients[i].addEdge("Knows", clients[i - 1]);

          // CREATE A REGULAR EDGE
          final Edge edge = clients[i].addEdge("Buy", product, new Object[] { "price", 1000 * i });
        }
      } finally {
        graph.shutdown();
      }

      graph = factory.getNoTx();
      try {
        for (int i = 0; i < clients.length; ++i)
          System.out.println("Created vertex " + i + ": " + clients[i].getRecord());

        for (int i = 0; i < clients.length; ++i) {
          // CREATE A REGULAR EDGE
          Iterable<OrientEdge> result = graph.command(
              new OCommandSQL("create edge Loves from " + clients[i].getIdentity() + " to " + fishing.getIdentity()
                  + " set real = true retry 10")).execute();

          Assert.assertTrue(result.iterator().hasNext());
          OrientEdge e = result.iterator().next();
          Assert.assertEquals(e.getProperty("real"), true);

          final OrientVertex explain = graph.command(new OCommandSQL("explain select from " + e.getIdentity())).execute();
          System.out
              .println("explain select from " + e.getIdentity() + " -> " + ((ODocument) explain.getRecord()).field("servers"));

          result = graph.command(new OCommandSQL("select from " + e.getIdentity())).execute();

          Assert.assertTrue(result.iterator().hasNext());
          OrientEdge e2 = result.iterator().next();
          Assert.assertEquals(e2.getProperty("real"), true);

        }
      } finally {
        graph.shutdown();
      }

      // FOR ALL THE DATABASES QUERY THE SINGLE CLUSTER TO TEST ROUTING
      for (int server = 0; server < clients.length; ++server) {
        OrientGraphFactory f = new OrientGraphFactory("plocal:target/server" + server + "/databases/" + getDatabaseName());
        OrientGraphNoTx g = f.getNoTx();

        System.out.println("Query from server " + server + "...");

        try {
          for (int cluster = 0; cluster < clients.length; ++cluster) {
            final String query = "select from cluster:client_" + cluster;

            final OrientVertex explain = g.command(new OCommandSQL("explain " + query)).execute();
            System.out.println("explain " + query + " -> " + ((ODocument) explain.getRecord()).field("servers"));

            Iterable<OrientVertex> result = g.command(new OCommandSQL(query)).execute();
            Assert.assertTrue("Error on query against 'cluster_" + cluster + "' on server '" + server + "': " + query, result
                .iterator().hasNext());

            OrientVertex v = result.iterator().next();

            Assert.assertEquals("Returned clients name property is != shard_" + cluster + " on server " + server, "shard_"
                + cluster, v.getProperty("name"));

            final Iterable<Vertex> knows = v.getVertices(Direction.OUT, "Knows");

            final Iterable<Vertex> boughtV = v.getVertices(Direction.OUT, "Buy");
            Assert.assertTrue(boughtV.iterator().hasNext());
            Assert.assertEquals(boughtV.iterator().next(), product);

            final Iterable<Edge> boughtE = v.getEdges(Direction.OUT, "Buy");
            Assert.assertNotNull(boughtE.iterator().next().getProperty("price"));
          }
        } finally {
          graph.shutdown();
        }
      }

      // TEST DISTRIBUTED QUERY + AGGREGATION + SUB_QUERY AGAINST ALL 3 DATABASES TO TEST MAP/REDUCE
      for (int server = 0; server < clients.length; ++server) {
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

          Assert.assertEquals("Returned wrong clients count on server " + server, 1, count);

        } finally {
          g.shutdown();
        }
      }

      // TEST DISTRIBUTED QUERY AGAINST ALL 3 DATABASES TO TEST MAP/REDUCE
      for (int server = 0; server < clients.length; ++server) {
        OrientGraphFactory f = new OrientGraphFactory("plocal:target/server" + server + "/databases/" + getDatabaseName());
        OrientGraphNoTx g = f.getNoTx();
        try {

          Iterable<OrientVertex> result = g.command(new OCommandSQL("select from Client")).execute();
          int count = 0;
          for (OrientVertex v : result) {
            count++;

            final Iterable<Vertex> knows = v.getVertices(Direction.OUT, "Knows");

            final Iterable<Vertex> boughtV = v.getVertices(Direction.OUT, "Buy");
            Assert.assertTrue(boughtV.iterator().hasNext());
            Assert.assertEquals(boughtV.iterator().next(), product);

            final Iterable<Edge> boughtE = v.getEdges(Direction.OUT, "Buy");
            Assert.assertNotNull(boughtE.iterator().next().getProperty("price"));
          }

          Assert.assertEquals("Returned wrong clients count on server " + server, 3, count);
        } finally {
          g.shutdown();
        }
      }

      // TEST DISTRIBUTED QUERY AGAINST ALL 3 DATABASES TO TEST AGGREGATION
      for (int server = 0; server < clients.length; ++server) {
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

          Assert.assertEquals("Returned wrong clients count on server " + server, 1, count);
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

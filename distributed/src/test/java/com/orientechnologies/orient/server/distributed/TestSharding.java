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
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

public class TestSharding extends AbstractServerClusterTest {

  protected final static int SERVERS     = 3;
  protected OrientVertex[]   vertices;
  protected int[]            versions;
  protected long             totalAmount = 0;

  @Test
  public void test() throws Exception {
    init(SERVERS);
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
      OrientGraphFactory localFactory = new OrientGraphFactory("plocal:target/server0/databases/" + getDatabaseName());
      OrientGraphNoTx graphNoTx = localFactory.getNoTx();

      try {
        final OrientVertexType clientType = graphNoTx.createVertexType("Client");
        for (int i = 1; i < serverInstance.size(); ++i) {
          final String serverName = serverInstance.get(i).getServerInstance().getDistributedManager().getLocalNodeName();
          clientType.addCluster("client_" + serverName);
        }

        graphNoTx.createVertexType("Product");
        graphNoTx.createVertexType("Hobby");

        graphNoTx.createEdgeType("Knows");
        graphNoTx.createEdgeType("Buy");
        graphNoTx.createEdgeType("Loves");

        Thread.sleep(500);
      } finally {
        graphNoTx.shutdown();
      }

      final OrientVertex product;
      final OrientVertex fishing;

      OrientBaseGraph graph = localFactory.getTx();
      try {
        product = graph.addVertex("class:Product");

        fishing = graph.addVertex("class:Hobby");
        fishing.setProperty("name", "Fishing");
      } finally {
        graph.shutdown();
      }

      Assert.assertEquals(product.getRecord().getVersion(), 1);
      Assert.assertEquals(fishing.getRecord().getVersion(), 1);

      versions = new int[serverInstance.size()];
      vertices = new OrientVertex[serverInstance.size()];

      for (int i = 0; i < vertices.length; ++i) {
        final String nodeName = serverInstance.get(i).getServerInstance().getDistributedManager().getLocalNodeName();

        OrientGraphFactory factory = new OrientGraphFactory("plocal:target/server" + i + "/databases/" + getDatabaseName());
        graph = factory.getNoTx();
        try {

          vertices[i] = graph.addVertex("class:Client");

          final int clId = vertices[i].getIdentity().getClusterId();

          if (i == 0)
            Assert.assertEquals("Error on assigning cluster client", clId, graph.getRawGraph().getClusterIdByName("client"));
          else {
            final int clusterId = graph.getRawGraph().getClusterIdByName("client_" + nodeName);
            Assert.assertEquals("Error on assigning cluster client_" + nodeName, clId, clusterId);
          }

          vertices[i].setProperty("name", "shard_" + i);

          long amount = i * 10000;
          vertices[i].setProperty("amount", amount);

          totalAmount += amount;

          System.out.println("Create vertex, class: " + vertices[i].getLabel() + ", cluster: " + clId + " -> "
              + vertices[i].getRecord());

          if (i > 1)
            // CREATE A LIGHT-WEIGHT EDGE
            vertices[i].addEdge("Knows", vertices[i - 1]);

          // CREATE A REGULAR EDGE
          final Edge edge = vertices[i].addEdge("Buy", product, new Object[] { "price", 1000 * i });

        } finally {
          graph.shutdown();
        }

      }

      // CHECK VERSIONS
      for (int i = 0; i < vertices.length; ++i) {
        versions[i] = vertices[i].getRecord().getVersion();
        Assert.assertTrue(versions[i] > 1);
      }

      graph = localFactory.getNoTx();
      try {
        for (int i = 0; i < vertices.length; ++i)
          System.out.println("Created vertex " + i + ": " + vertices[i].getRecord());
      } finally {
        graph.shutdown();
      }

      for (int i = 0; i < vertices.length; ++i) {
        OrientGraphFactory factory = new OrientGraphFactory("plocal:target/server" + i + "/databases/" + getDatabaseName());
        graph = factory.getNoTx();
        try {

          // CREATE A REGULAR EDGE
          Iterable<OrientEdge> result = graph.command(
              new OCommandSQL("create edge Loves from " + vertices[i].getIdentity() + " to " + fishing.getIdentity()
                  + " set real = true")).execute();

          Assert.assertTrue(result.iterator().hasNext());
          OrientEdge e = result.iterator().next();
          Assert.assertEquals(e.getProperty("real"), true);

          Assert.assertEquals(2, e.getRecord().getVersion());
          e.getOutVertex().getRecord().reload();
          Assert.assertEquals(versions[i] + 1, e.getOutVertex().getRecord().getVersion());

          e.getInVertex().getRecord().reload();
//          Assert.assertEquals(fishing.getRecord().getVersion() + i + 1, e.getInVertex().getRecord().getVersion());

          final OrientVertex explain = graph.command(new OCommandSQL("explain select from " + e.getIdentity())).execute();
          System.out
              .println("explain select from " + e.getIdentity() + " -> " + ((ODocument) explain.getRecord()).field("servers"));

          result = graph.command(new OCommandSQL("select from " + e.getIdentity())).execute();

          Assert.assertTrue(result.iterator().hasNext());
          OrientEdge e2 = result.iterator().next();
          Assert.assertEquals(e2.getProperty("real"), true);

        } finally {
          graph.shutdown();
        }
      }

      // FOR ALL THE DATABASES QUERY THE SINGLE CLUSTER TO TEST ROUTING
      for (int server = 0; server < vertices.length; ++server) {
        OrientGraphFactory f = new OrientGraphFactory("plocal:target/server" + server + "/databases/" + getDatabaseName());
        OrientGraphNoTx g = f.getNoTx();

        System.out.println("Query from server " + server + "...");

        try {
          for (int i = 0; i < vertices.length; ++i) {
            final String nodeName = serverInstance.get(i).getServerInstance().getDistributedManager().getLocalNodeName();

            String clusterName = "client";
            if (i > 0)
              clusterName += "_" + nodeName;

            String query = "select from cluster:" + clusterName;

            final Object explain = g.getRawGraph().command(new OCommandSQL("explain " + query)).execute();
            System.out.println("explain " + query + " -> " + explain);

            Iterable<OrientVertex> result = g.command(new OCommandSQL(query)).execute();
            Assert.assertTrue("Error on query against '" + clusterName + "' on server '" + server + "': " + query, result
                .iterator().hasNext());

            OrientVertex v = result.iterator().next();

            Assert.assertEquals("Returned vertices name property is != shard_" + i + " on server " + server, "shard_" + i,
                v.getProperty("name"));

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
      for (int server = 0; server < vertices.length; ++server) {
        OrientGraphFactory f = new OrientGraphFactory("plocal:target/server" + 0 + "/databases/" + getDatabaseName());
        OrientGraphNoTx g = f.getNoTx();
        try {
          // MISC QUERIES
          Iterable<OrientVertex> result = g.command(new OCommandSQL("select sum(amount) from ( select from Client )")).execute();

          int count = 0;
          for (OrientVertex v : result) {
            System.out.println("select sum(amount) from ( select from Client ) -> " + v.getRecord());

            Assert
                .assertEquals("Returned wrong sum of amount on server " + server, (Long) totalAmount, (Long) v.getProperty("sum"));

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
          for (OrientVertex v : result) {
            count++;

            final Iterable<Vertex> knows = v.getVertices(Direction.OUT, "Knows");

            final Iterable<Vertex> boughtV = v.getVertices(Direction.OUT, "Buy");
            Assert.assertTrue(boughtV.iterator().hasNext());
            Assert.assertEquals(boughtV.iterator().next(), product);

            final Iterable<Edge> boughtE = v.getEdges(Direction.OUT, "Buy");
            Assert.assertNotNull(boughtE.iterator().next().getProperty("price"));
          }

          Assert.assertEquals("Returned wrong vertices count on server " + server, SERVERS, count);
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

      // TEST DISTRIBUTED DELETE WITH DIRECT COMMAND AND SQL
      OrientGraphFactory f = new OrientGraphFactory("plocal:target/server" + 0 + "/databases/" + getDatabaseName());
      OrientGraphNoTx g = f.getNoTx();
      try {
        Iterable<OrientVertex> countResultBeforeDelete = g.command(new OCommandSQL("select from Client")).execute();
        long totalBeforeDelete = 0;
        for (OrientVertex v : countResultBeforeDelete)
          totalBeforeDelete++;

        Iterable<OrientVertex> result = g.command(new OCommandSQL("select from Client")).execute();

        int count = 0;

        for (OrientVertex v : result) {
          if (count % 2 == 0) {
            // DELETE ONLY EVEN INSTANCES
            v.remove();
            count++;
          }
        }

        Iterable<OrientVertex> countResultAfterDelete = g.command(new OCommandSQL("select from Client")).execute();
        long totalAfterDelete = 0;
        for (OrientVertex v : countResultAfterDelete)
          totalAfterDelete++;

        Assert.assertEquals(totalBeforeDelete - count, totalAfterDelete);

        g.command(new OCommandSQL("create vertex Client set name = 'temp1'")).execute();
        g.command(new OCommandSQL("create vertex Client set name = 'temp2'")).execute();
        g.command(new OCommandSQL("create vertex Client set name = 'temp3'")).execute();

        g.command(new OCommandSQL("delete vertex Client")).execute();

        Iterable<OrientVertex> countResultAfterFullDelete = g.command(new OCommandSQL("select from Client")).execute();
        long totalAfterFullDelete = 0;
        for (OrientVertex v : countResultAfterFullDelete)
          totalAfterFullDelete++;

        Assert.assertEquals(0, totalAfterFullDelete);

      } finally {
        g.shutdown();
      }

      OrientVertex v1, v2;
      OrientGraph gTx = f.getTx();
      try {
        v1 = gTx.addVertex("class:Client");
        v1.setProperty("name", "test1");

        v2 = gTx.addVertex("class:Client");
        v2.setProperty("name", "test1");
      } finally {
        gTx.shutdown();
      }

      gTx = f.getTx();
      try {
        // DELETE IN TX
        v1.remove();
        v2.remove();
      } finally {
        gTx.shutdown();
      }

      gTx = f.getTx();
      try {
        Iterable<OrientVertex> countResultAfterFullDelete = gTx.command(new OCommandSQL("select from Client")).execute();
        long totalAfterFullDelete = 0;
        for (OrientVertex v : countResultAfterFullDelete)
          totalAfterFullDelete++;

        Assert.assertEquals(0, totalAfterFullDelete);
      } finally {
        gTx.shutdown();
      }

    } catch (Exception e) {
      e.printStackTrace();

      // WAIT FOR TERMINATION
      Thread.sleep(10000);
      throw e;
    }
  }
}

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

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.setup.ServerRun;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class ShardingIT extends AbstractServerClusterTest {

  protected static final int SERVERS = 3;
  protected OVertex[] vertices;
  protected int[] versions;
  protected long totalAmount = 0;

  @Test
  @Ignore
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
  protected void onAfterDatabaseCreation(ODatabaseDocument graphNoTx) {
    graphNoTx.command("create class `Client-Type` extends V clusters 1");
    final OClass clientType = graphNoTx.getClass("Client-Type");
    final OProperty prop = clientType.createProperty("name-property", OType.STRING);
    prop.createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    graphNoTx
        .command(new OCommandSQL("alter cluster `Client-Type` name `Client-Type_europe`"))
        .execute();

    clientType.addCluster("client-type_usa");
    clientType.addCluster("client-type_asia");

    graphNoTx.command("create class `Product-Type` extends V clusters 1");
    graphNoTx.command("create class `Hobby-Type` extends V clusters 1");

    graphNoTx.command("create class `Knows-Type` extends E clusters 1");
    graphNoTx.command("create class `Buy-Type` extends E clusters 1");
    graphNoTx.command("create class `Loves-Type` extends E clusters 1");
  }

  @Override
  protected void executeTest() throws Exception {
    try {
      final OVertex product;
      final OVertex fishing;

      ODatabaseDocument graph =
          serverInstance
              .get(0)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "admin");
      graph.begin();

      try {
        product = graph.newVertex("Product-Type");
        product.save();
        fishing = graph.newVertex("Hobby-Type");
        fishing.setProperty("name-property", "Fishing");
        fishing.save();
        graph.commit();
      } finally {
        graph.close();
      }

      Assert.assertEquals(product.getRecord().getVersion(), 1);
      Assert.assertEquals(fishing.getRecord().getVersion(), 1);

      versions = new int[serverInstance.size()];
      vertices = new OVertex[serverInstance.size()];

      for (int i = 0; i < vertices.length; ++i) {
        final String nodeName =
            serverInstance.get(i).getServerInstance().getDistributedManager().getLocalNodeName();

        graph =
            serverInstance
                .get(i)
                .getServerInstance()
                .openDatabase(getDatabaseName(), "admin", "admin");
        try {

          vertices[i] = graph.newVertex("Client-Type");
          vertices[i].save();

          final int clId = vertices[i].getIdentity().getClusterId();

          final String expectedClusterNameAssigned = "client-type_" + nodeName;
          final int clusterId = graph.getClusterIdByName(expectedClusterNameAssigned);
          Assert.assertEquals("Error on assigning cluster client_" + nodeName, clId, clusterId);

          vertices[i].setProperty("name-property", "shard_" + i);
          vertices[i].setProperty("blob", new byte[1000]);

          long amount = i * 10000;
          vertices[i].setProperty("amount", amount);

          totalAmount += amount;

          System.out.println(
              "Create vertex, class: "
                  + vertices[i].getSchemaType().get().getName()
                  + ", cluster: "
                  + clId
                  + " -> "
                  + vertices[i].getRecord());

          if (i > 1) {
            // CREATE A LIGHT-WEIGHT EDGE
            final OEdge e = vertices[i].addEdge(vertices[i - 1], "Knows-Type");
            e.setProperty("blob", new byte[1000]);
            e.save();
          }

          // CREATE A REGULAR EDGE
          final OEdge edge = vertices[i].addEdge(product, "Buy-Type");
          edge.setProperty("price", 1000 * i);
          edge.save();

        } finally {
          graph.close();
        }
      }

      // CHECK VERSIONS
      for (int i = 0; i < vertices.length; ++i) {
        versions[i] = vertices[i].getRecord().getVersion();
        Assert.assertTrue(versions[i] > 1);
      }

      graph =
          serverInstance
              .get(0)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "admin");
      try {
        for (int i = 0; i < vertices.length; ++i)
          System.out.println("Created vertex " + i + ": " + vertices[i].getRecord());
      } finally {
        graph.close();
      }

      for (int i = 0; i < vertices.length; ++i) {

        graph =
            serverInstance
                .get(i)
                .getServerInstance()
                .openDatabase(getDatabaseName(), "admin", "admin");
        try {

          // CREATE A REGULAR EDGE
          Iterable<OEdge> result =
              graph
                  .command(
                      new OCommandSQL(
                          "create edge `Loves-Type` from "
                              + vertices[i].getIdentity()
                              + " to "
                              + fishing.getIdentity()
                              + " set real = true"))
                  .execute();

          Assert.assertTrue(result.iterator().hasNext());
          OEdge e = result.iterator().next();
          Assert.assertEquals(e.getProperty("real"), true);

          Assert.assertEquals(1, e.getRecord().getVersion());
          e.getFrom().getRecord().reload();
          Assert.assertEquals(versions[i] + 1, e.getFrom().getRecord().getVersion());

          e.getTo().getRecord().reload();
          Assert.assertEquals(
              fishing.getRecord().getVersion() + i + 1, e.getTo().getRecord().getVersion());

          final Iterable<OElement> explain =
              graph.command(new OCommandSQL("explain select from " + e.getIdentity())).execute();

          System.out.println(
              "explain select from "
                  + e.getIdentity()
                  + " -> "
                  + ((ODocument) explain.iterator().next().getRecord()).field("servers"));

          result = graph.command(new OCommandSQL("select from " + e.getIdentity())).execute();

          Assert.assertTrue(result.iterator().hasNext());
          OElement e2 = result.iterator().next();
          Assert.assertEquals(e2.getProperty("real"), true);

        } finally {
          graph.close();
        }
      }

      // FOR ALL THE DATABASES QUERY THE SINGLE CLUSTER TO TEST ROUTING
      for (int server = 0; server < vertices.length; ++server) {
        ODatabaseDocument g =
            serverInstance
                .get(server)
                .getServerInstance()
                .openDatabase(getDatabaseName(), "admin", "admin");

        System.out.println("Query from server " + server + "...");

        try {
          for (int i = 0; i < vertices.length; ++i) {
            final String nodeName =
                serverInstance
                    .get(i)
                    .getServerInstance()
                    .getDistributedManager()
                    .getLocalNodeName();

            String clusterName = "client-Type";
            clusterName += "_" + nodeName;

            String query = "select from `cluster:" + clusterName + "`";

            final Object explain = g.command(new OCommandSQL("explain " + query)).execute();
            System.out.println("explain " + query + " -> " + explain);

            Iterable<OVertex> result = g.command(new OCommandSQL(query)).execute();
            Assert.assertTrue(
                "Error on query against '" + clusterName + "' on server '" + server + "': " + query,
                result.iterator().hasNext());

            OElement v = result.iterator().next();

            Assert.assertEquals(
                "Returned vertices name property is != shard_" + i + " on server " + server,
                "shard_" + i,
                v.getProperty("name-property"));

            final Iterable<OVertex> knows =
                v.asVertex().get().getVertices(ODirection.OUT, "Knows-Type");

            final Iterable<OVertex> boughtV =
                v.asVertex().get().getVertices(ODirection.OUT, "Buy-Type");
            Assert.assertTrue(boughtV.iterator().hasNext());
            Assert.assertEquals(boughtV.iterator().next(), product);

            final Iterable<OEdge> boughtE = v.asVertex().get().getEdges(ODirection.OUT, "Buy-Type");
            Assert.assertNotNull(boughtE.iterator().next().getProperty("price"));
          }
        } finally {
          g.close();
        }
      }

      // TEST DISTRIBUTED QUERY + AGGREGATION + SUB_QUERY AGAINST ALL 3 DATABASES TO TEST MAP/REDUCE
      for (int server = 0; server < vertices.length; ++server) {

        ODatabaseDocument g =
            serverInstance
                .get(0)
                .getServerInstance()
                .openDatabase(getDatabaseName(), "admin", "admin");
        try {
          // MISC QUERIES
          Iterable<OElement> result =
              g.command(
                      new OCommandSQL(
                          "select sum(amount), set(amount) from ( select from `Client-type` )"))
                  .execute();

          int count = 0;
          for (OElement v : result) {
            System.out.println(
                "select sum(amount), set(amount) from ( select from `Client-Type` ) -> "
                    + v.getRecord());

            Assert.assertNotNull(
                "set() function wasn't returned on server " + server, v.getProperty("set"));

            Assert.assertEquals(
                "Returned wrong sum of amount on server " + server,
                (Long) totalAmount,
                (Long) v.getProperty("sum"));

            count++;
          }

          Assert.assertEquals("Returned wrong vertices count on server " + server, 1, count);

        } finally {
          g.close();
        }
      }

      // TEST DISTRIBUTED QUERY AGAINST ALL 3 DATABASES TO TEST MAP/REDUCE
      for (int server = 0; server < vertices.length; ++server) {
        ODatabaseDocument g =
            serverInstance
                .get(server)
                .getServerInstance()
                .openDatabase(getDatabaseName(), "admin", "admin");
        try {

          Iterable<OElement> result =
              g.command(new OCommandSQL("select from `Client-Type`")).execute();
          int count = 0;
          for (OElement v : result) {
            count++;

            final Iterable<OVertex> knows =
                v.asVertex().get().getVertices(ODirection.OUT, "Knows-Type");

            final Iterable<OVertex> boughtV =
                v.asVertex().get().getVertices(ODirection.OUT, "Buy-Type");
            Assert.assertTrue(boughtV.iterator().hasNext());
            Assert.assertEquals(boughtV.iterator().next(), product);

            final Iterable<OEdge> boughtE = v.asVertex().get().getEdges(ODirection.OUT, "Buy-Type");
            Assert.assertNotNull(boughtE.iterator().next().getProperty("price"));
          }

          Assert.assertEquals("Returned wrong vertices count on server " + server, SERVERS, count);
        } finally {
          g.close();
        }
      }

      // TEST DISTRIBUTED QUERY AGAINST ALL 3 DATABASES TO TEST AGGREGATION
      for (int server = 0; server < vertices.length; ++server) {
        ODatabaseDocument g =
            serverInstance
                .get(server)
                .getServerInstance()
                .openDatabase(getDatabaseName(), "admin", "admin");
        try {

          Iterable<OElement> result =
              g.command(
                      new OCommandSQL(
                          "select max(amount), avg(amount), sum(amount) from `Client-Type`"))
                  .execute();

          int count = 0;
          for (OElement v : result) {
            System.out.println(
                "select max(amount), avg(amount), sum(amount) from Client-Type -> "
                    + v.getRecord());
            count++;
          }

          Assert.assertEquals("Returned wrong vertices count on server " + server, 1, count);

        } finally {
          g.close();
        }
      }

      // TEST DISTRIBUTED QUERY AGAINST ALL 3 DATABASES TO TEST AGGREGATION + GROUP BY
      for (int server = 0; server < vertices.length; ++server) {
        ODatabaseDocument g =
            serverInstance
                .get(server)
                .getServerInstance()
                .openDatabase(getDatabaseName(), "admin", "admin");
        try {

          Iterable<OElement> result =
              g.command(
                      new OCommandSQL(
                          "select name-property, count(*) from `Client-Type` group by `name-property`"))
                  .execute();

          int count = 0;
          for (OElement v : result) {
            System.out.println(
                "select `name-property`, count(*) from Client-Type group by `name-property` -> "
                    + v.getRecord());

            Assert.assertEquals(((Number) v.getProperty("count")).intValue(), 1);

            count++;
          }

          Assert.assertEquals(
              "Returned wrong vertices count on server " + server, vertices.length, count);
        } finally {
          g.close();
        }
      }

      // TEST DISTRIBUTED QUERY AGAINST ALL 3 DATABASES TO TEST AGGREGATION + ADDITIONAL FIELD
      for (int server = 0; server < vertices.length; ++server) {
        ODatabaseDocument g =
            serverInstance
                .get(server)
                .getServerInstance()
                .openDatabase(getDatabaseName(), "admin", "admin");
        try {

          Iterable<OElement> result =
              g.command(new OCommandSQL("select `name-property`, count(*) from `Client-Type`"))
                  .execute();

          int count = 0;
          for (OElement v : result) {
            System.out.println(
                "select `name-property`, count(*) from Client-Type -> " + v.getRecord());

            Assert.assertEquals(((Number) v.getProperty("count")).intValue(), vertices.length);
            Assert.assertNotNull(v.getProperty("name-property"));

            count++;
          }

          Assert.assertEquals("Returned wrong vertices count on server " + server, 1, count);
        } finally {
          g.close();
        }
      }
      testQueryWithFilter();

      // TEST DISTRIBUTED DELETE WITH DIRECT COMMAND AND SQL
      ODatabaseDocument g =
          serverInstance
              .get(0)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "admin");
      try {
        Iterable<OElement> countResultBeforeDelete =
            g.command(new OCommandSQL("select from `Client-Type`")).execute();
        long totalBeforeDelete = 0;
        for (OElement v : countResultBeforeDelete) totalBeforeDelete++;

        Iterable<OElement> result =
            g.command(new OCommandSQL("select from `Client-Type`")).execute();

        int count = 0;

        for (OElement v : result) {
          if (count % 2 == 0) {
            // DELETE ONLY EVEN INSTANCES
            v.delete();
            count++;
          }
        }

        Iterable<OElement> countResultAfterDelete =
            g.command(new OCommandSQL("select from `Client-type`")).execute();
        long totalAfterDelete = 0;
        for (OElement v : countResultAfterDelete) totalAfterDelete++;

        Assert.assertEquals(totalBeforeDelete - count, totalAfterDelete);

        g.command(new OCommandSQL("create vertex `Client-Type` set `name-property` = 'temp1'"))
            .execute();
        g.command(new OCommandSQL("create vertex `Client-Type` set `name-property` = 'temp2'"))
            .execute();
        g.command(new OCommandSQL("create vertex `Client-Type` set `name-property` = 'temp3'"))
            .execute();

        Iterable<OElement> countResultAfterFullDelete =
            g.command(new OCommandSQL("select from `Client-Type`")).execute();
        long totalAfterFullDelete = 0;
        for (OElement v : countResultAfterFullDelete) totalAfterFullDelete++;

        Assert.assertEquals(0, totalAfterFullDelete);

      } finally {
        g.close();
      }

      OVertex v1, v2;
      ODatabaseDocument gTx =
          serverInstance
              .get(0)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "admin");
      gTx.begin();
      try {
        v1 = gTx.newVertex("Client-Type");
        v1.setProperty("name-property", "test1");
        v1.save();

        v2 = gTx.newVertex("Client-Type");
        v2.setProperty("name-property", "test1");
        v2.save();
        gTx.commit();
      } finally {
        gTx.close();
      }

      gTx =
          serverInstance
              .get(0)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "admin");
      gTx.begin();
      try {
        // DELETE IN TX
        v1.delete();
        v2.delete();
        gTx.commit();
      } finally {
        gTx.close();
      }

      gTx =
          serverInstance
              .get(0)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "admin");
      gTx.begin();
      try {
        Iterable<OElement> countResultAfterFullDelete =
            gTx.command(new OCommandSQL("select from `Client-Type`")).execute();
        long totalAfterFullDelete = 0;
        for (OElement v : countResultAfterFullDelete) totalAfterFullDelete++;

        Assert.assertEquals(0, totalAfterFullDelete);
        gTx.commit();
      } finally {
        gTx.close();
      }

    } catch (Exception e) {
      e.printStackTrace();

      // WAIT FOR TERMINATION
      Thread.sleep(2000);
      throw e;
    }
  }

  private void testQueryWithFilter() {
    // TEST DISTRIBUTED QUERY AGAINST ALL 3 DATABASES TO TEST AGGREGATION + ADDITIONAL FIELD
    for (int server = 0; server < vertices.length; ++server) {
      ODatabaseDocument g =
          serverInstance
              .get(server)
              .getServerInstance()
              .openDatabase(getDatabaseName(), "admin", "admin");
      try {

        Iterable<OElement> result =
            g.command(
                    new OCommandSQL(
                        "select * from `Client-Type` where `name-property` = 'shard_"
                            + server
                            + "'"))
                .execute();

        int count = 0;
        for (OElement v : result) {
          System.out.println(
              "select * from `Client-Type` where `name-property` = 'shard_"
                  + server
                  + "' ->"
                  + v.getRecord());

          Assert.assertNotNull(v.getProperty("name-property"));

          count++;
        }

        Assert.assertTrue("Returned wrong vertices count on server " + server, count > 0);
      } finally {
        g.close();
      }
    }
  }
}

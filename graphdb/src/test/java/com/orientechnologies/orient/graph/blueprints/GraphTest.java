/*
 *
 *  * Copyright 2010-2014 OrientDB LTD (info(-at-)orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.graph.blueprints;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class GraphTest {

  public static final String URL = "memory:" + GraphTest.class.getSimpleName();

  @BeforeClass
  public static void beforeClass() {
    OrientGraph g = new OrientGraph(URL, "admin", "admin");
    g.drop();
  }

  @Test
  public void indexes() {
    OrientGraph g = new OrientGraph(URL, "admin", "admin");

    try {
      if (g.getVertexType("VC1") == null) {
        g.createVertexType("VC1");
      }
    } finally {
      g.shutdown();
    }
    g = new OrientGraph(URL, "admin", "admin");
    try {
      // System.out.println(g.getIndexedKeys(Vertex.class,true)); this will print VC1.p1
      if (g.getIndex("VC1.p1", Vertex.class) == null) { // this will return null. I do not know why
        g.createKeyIndex(
            "p1",
            Vertex.class,
            new Parameter<String, String>("class", "VC1"),
            new Parameter<String, String>("type", "UNIQUE"),
            new Parameter<String, OType>("keytype", OType.STRING));
      }
    } catch (OIndexException e) {
      // ignore because the index may exist
    } finally {
      g.shutdown();
    }

    g = new OrientGraph(URL, "admin", "admin");
    String val1 = System.currentTimeMillis() + "";
    try {
      Vertex v = g.addVertex("class:VC1");
      v.setProperty("p1", val1);
    } finally {
      g.shutdown();
    }
    g = new OrientGraph(URL, "admin", "admin");
    try {
      Vertex v = g.addVertex("class:VC1");
      v.setProperty("p1", val1);
    } finally {
      try {
        g.shutdown();
        fail("must throw duplicate key here!");
      } catch (ORecordDuplicatedException e) {
        // ok
      }
    }
  }

  @Test
  public void testIndexCollate() {
    OrientGraph g = new OrientGraph(URL, "admin", "admin");

    OrientVertexType vCollate = g.createVertexType("VCollate");

    vCollate.createProperty("name", OType.STRING);

    g.createKeyIndex(
        "name",
        Vertex.class,
        new Parameter<String, String>("class", "VCollate"),
        new Parameter<String, String>("type", "UNIQUE"),
        new Parameter<String, OType>("keytype", OType.STRING),
        new Parameter<String, String>("collate", "ci"));
    OrientVertex vertex = g.addVertex("class:VCollate", new Object[] {"name", "Enrico"});

    g.commit();

    Iterable<Vertex> enrico = g.getVertices("VCollate.name", "ENRICO");

    Assert.assertEquals(true, enrico.iterator().hasNext());
  }

  @Test
  public void testGetCompositeKeyBySingleValue() {
    OrientGraph g = new OrientGraph(URL, "admin", "admin");

    OrientVertexType vComposite = g.createVertexType("VComposite");
    vComposite.createProperty("login", OType.STRING);
    vComposite.createProperty("permissions", OType.EMBEDDEDSET, OType.STRING);

    vComposite.createIndex(
        "VComposite_Login_Perm", OClass.INDEX_TYPE.UNIQUE, "login", "permissions");

    String loginOne = "admin";
    Set<String> permissionsOne = new HashSet<String>();
    permissionsOne.add("perm1");
    permissionsOne.add("perm2");

    String loginTwo = "user";
    Set<String> permissionsTwo = new HashSet<String>();
    permissionsTwo.add("perm3");
    permissionsTwo.add("perm4");

    g.addVertex("class:VComposite", "login", loginOne, "permissions", permissionsOne);
    g.commit();

    g.addVertex("class:VComposite", "login", loginTwo, "permissions", permissionsTwo);
    g.commit();

    Iterable<Vertex> vertices =
        g.getVertices("VComposite", new String[] {"login"}, new String[] {"admin"});
    Iterator<Vertex> verticesIterator = vertices.iterator();

    Assert.assertTrue(verticesIterator.hasNext());
    Vertex vertex = verticesIterator.next();
    Assert.assertEquals(vertex.getProperty("login"), "admin");
    Assert.assertEquals(vertex.getProperty("permissions"), permissionsOne);
  }

  @Test
  public void testEmbeddedListAsVertexProperty() {
    OrientGraph g = new OrientGraph(URL, "admin", "admin");

    try {
      OrientVertexType vertexType = g.createVertexType("EmbeddedClass");
      vertexType.createProperty("embeddedList", OType.EMBEDDEDLIST);

      OrientVertex vertex = g.addVertex("class:EmbeddedClass");

      List<ODocument> embeddedList = new ArrayList<ODocument>();
      ODocument docOne = new ODocument();
      docOne.field("prop", "docOne");

      ODocument docTwo = new ODocument();
      docTwo.field("prop", "docTwo");

      embeddedList.add(docOne);
      embeddedList.add(docTwo);

      vertex.setProperty("embeddedList", embeddedList);

      final Object id = vertex.getId();

      g.shutdown();
      g = new OrientGraph(URL, "admin", "admin");

      vertex = g.getVertex(id);
      embeddedList = vertex.getProperty("embeddedList");

      docOne = embeddedList.get(0);
      Assert.assertEquals(docOne.field("prop"), "docOne");

      docTwo = embeddedList.get(1);
      Assert.assertEquals(docTwo.field("prop"), "docTwo");
    } finally {
      g.shutdown();
    }
  }

  @Test
  public void testGetEdgesUpdate() {
    OrientGraph g = new OrientGraph(URL, "admin", "admin");
    try {
      g.createVertexType("GetEdgesUpdate");
      g.createEdgeType("getEdgesUpdateEdge");

      OrientVertex vertexOne = g.addVertex("class:GetEdgesUpdate");

      OrientVertex vertexTwo = g.addVertex("class:GetEdgesUpdate");
      OrientVertex vertexThree = g.addVertex("class:GetEdgesUpdate");
      OrientVertex vertexFour = g.addVertex("class:GetEdgesUpdate");

      vertexOne.addEdge("getEdgesUpdateEdge", vertexTwo);
      vertexOne.addEdge("getEdgesUpdateEdge", vertexThree);
      vertexOne.addEdge("getEdgesUpdateEdge", vertexFour);

      g.commit();

      Iterable<Edge> iterable = vertexOne.getEdges(Direction.OUT, "getEdgesUpdateEdge");
      Iterator<Edge> iterator = iterable.iterator();

      int counter = 0;
      while (iterator.hasNext()) {
        iterator.next();
        counter++;
      }

      Assert.assertEquals(3, counter);

      iterable = vertexOne.getEdges(Direction.OUT, "getEdgesUpdateEdge");
      iterator = iterable.iterator();

      Edge deleteEdge = (Edge) iterator.next();

      Vertex deleteVertex = deleteEdge.getVertex(Direction.IN);
      deleteVertex.remove();

      g.commit();

      iterable = vertexOne.getEdges(Direction.OUT, "getEdgesUpdateEdge");
      iterator = iterable.iterator();

      counter = 0;
      while (iterator.hasNext()) {
        iterator.next();
        counter++;
      }

      Assert.assertEquals(2, counter);
    } finally {
      g.shutdown();
    }
  }

  @Test
  public void testBrokenVertex1() {
    OrientGraph g = new OrientGraph(URL, "admin", "admin");
    try {
      g.createVertexType("BrokenVertex1V");
      g.createEdgeType("BrokenVertex1E");

      OrientVertex vertexOne = g.addVertex("class:BrokenVertex1V");

      OrientVertex vertexTwo = g.addVertex("class:BrokenVertex1V");

      vertexOne.addEdge("BrokenVertex1E", vertexTwo);

      g.commit();

      g.command(new OCommandSQL("delete from " + vertexTwo.getRecord().getIdentity() + " unsafe"))
          .execute();
      // g.command(new OCommandSQL("update BrokenVertex1E set out = null")).execute();

      g.shutdown();
      g = new OrientGraph(URL, "admin", "admin");
      Iterable<Vertex> iterable =
          g.command(new OCommandSQL("select from BrokenVertex1V")).execute();
      Iterator<Vertex> iterator = iterable.iterator();

      int counter = 0;
      while (iterator.hasNext()) {
        OrientVertex v = (OrientVertex) iterator.next();
        for (Vertex v1 : v.getVertices(Direction.OUT, "BrokenVertex1E")) {
          assertNotNull(((OrientVertex) v1).getRecord());
        }
      }

    } finally {
      g.shutdown();
    }
  }

  @Test
  public void shouldAddVertexAndEdgeInTheSameCluster() {
    OrientGraphFactory orientGraphFactory =
        new OrientGraphFactory("memory:shouldAddVertexAndEdgeInTheSameCluster");
    final OrientGraphNoTx graphDbNoTx = orientGraphFactory.getNoTx();
    try {
      OrientVertexType deviceVertex = graphDbNoTx.createVertexType("Device");
      OrientEdgeType edgeType = graphDbNoTx.createEdgeType("Link");

      edgeType.addCluster("Links");

      OrientVertex dev1 = graphDbNoTx.addVertex("Device");
      OrientVertex dev2 = graphDbNoTx.addVertex("Device");

      final OrientEdge e = graphDbNoTx.addEdge("class:Link,cluster:Links", dev1, dev2, null);

      Assert.assertEquals(
          e.getIdentity().getClusterId(), graphDbNoTx.getRawGraph().getClusterIdByName("Links"));

    } finally {
      graphDbNoTx.shutdown();
      orientGraphFactory.close();
    }
  }

  @Test
  public void testCustomPredicate() {
    OrientGraphFactory orientGraphFactory = new OrientGraphFactory("memory:testCustomPredicate");
    final OrientGraphNoTx g = orientGraphFactory.getNoTx();
    try {
      g.addVertex(null).setProperty("test", true);
      g.addVertex(null).setProperty("test", false);
      g.addVertex(null).setProperty("no", true);

      g.commit();

      GraphQuery query = g.query();
      query.has(
          "test",
          new Predicate() {
            @Override
            public boolean evaluate(Object first, Object second) {
              return first != null && first.equals(second);
            }
          },
          true);

      Iterable<Vertex> vertices = query.vertices();

      final Iterator<Vertex> it = vertices.iterator();
      Assert.assertTrue(it.hasNext());
      Assert.assertTrue((Boolean) it.next().getProperty("test"));
      Assert.assertFalse(it.hasNext());

    } finally {
      g.shutdown();
      orientGraphFactory.close();
    }
  }

  @Test
  public void testKebabCaseQuery() {
    OrientGraphFactory orientGraphFactory = new OrientGraphFactory("memory:testKebabCase");
    final OrientGraphNoTx g = orientGraphFactory.getNoTx();
    try {
      g.addVertex(null).setProperty("test-one", true);
      g.addVertex(null).setProperty("test-one", false);

      g.commit();

      GraphQuery query = g.query();
      query.has("test-one", true);

      Iterable<Vertex> vertices = query.vertices();

      final Iterator<Vertex> it = vertices.iterator();
      Assert.assertTrue(it.hasNext());
      Assert.assertTrue((Boolean) it.next().getProperty("test-one"));
      Assert.assertFalse(it.hasNext());

    } finally {
      g.shutdown();
      orientGraphFactory.close();
    }
  }

  @Test
  public void testIndexCreateDropCreate() {
    OrientGraph g = new OrientGraph(URL, "admin", "admin");
    try {
      g.createIndex("IndexCreateDropCreate", Vertex.class);
      g.dropIndex("IndexCreateDropCreate");
      g.createIndex("IndexCreateDropCreate", Vertex.class);
    } finally {
      g.shutdown();
    }
  }

  @Test
  public void testCompositeKey() {

    OrientGraphNoTx graph = new OrientGraphNoTx("memory:testComposite");

    try {
      if (!((ODatabaseInternal) graph.getRawGraph()).getStorage().isRemote()) {
        graph.createVertexType("Account");

        graph.command(new OCommandSQL("create property account.description STRING")).execute();
        graph.command(new OCommandSQL("create property account.namespace STRING")).execute();
        graph.command(new OCommandSQL("create property account.name STRING")).execute();
        graph
            .command(
                new OCommandSQL(
                    "create index account.composite on account (name, namespace) unique"))
            .execute();

        graph.addVertex(
            "class:account",
            new Object[] {"name", "foo", "namespace", "bar", "description", "foobar"});
        final OrientVertex vertex =
            graph.addVertex(
                "class:account",
                new Object[] {"name", "foo", "namespace", "baz", "description", "foobaz"});

        final OIndex index =
            graph
                .getRawGraph()
                .getMetadata()
                .getIndexManagerInternal()
                .getIndex(graph.getRawGraph(), "account.composite");
        try (Stream<ORID> rids = index.getInternal().getRids(new OCompositeKey("foo", "baz"))) {
          Assert.assertEquals(vertex.getIdentity(), rids.findAny().orElse(null));
        }
      }
    } finally {
      graph.drop();
    }
  }
}

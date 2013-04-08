package com.orientechnologies.orient.graph.gremlin;

import org.testng.Assert;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class BlueprintsTest {
  private static String DB_URL = "local:target/databases/tinkerpop";
  private OrientGraph   graph;

  public BlueprintsTest() {
  }

  @BeforeClass
  public void before() {
    graph = new OrientGraph(DB_URL);
  }

  @AfterClass
  public void after() {
    graph.shutdown();
  }

  @Test
  public void testSubVertex() {
    if (graph.getVertexType("SubVertex") == null)
      graph.createVertexType("SubVertex");

    Vertex v = graph.addVertex("class:SubVertex");
    v.setProperty("key", "subtype");
    Assert.assertEquals(((OrientVertex) v).getRecord().getSchemaClass().getName(), "SubVertex");
  }

  @Test
  public void testSubEdge() {
    if (graph.getEdgeType("SubEdge") == null)
      graph.createEdgeType("SubEdge");
    if (graph.getVertexType("SubVertex") == null)
      graph.createVertexType("SubVertex");

    Vertex v1 = graph.addVertex("class:SubVertex");
    v1.setProperty("key", "subtype+subedge");
    Assert.assertEquals(((OrientVertex) v1).getRecord().getSchemaClass().getName(), "SubVertex");

    Vertex v2 = graph.addVertex("class:SubVertex");
    v2.setProperty("key", "subtype+subedge");
    Assert.assertEquals(((OrientVertex) v2).getRecord().getSchemaClass().getName(), "SubVertex");

    Edge e = graph.addEdge("class:SubEdge", v1, v2, null);
    e.setProperty("key", "subedge");
    Assert.assertEquals(((OrientEdge) e).getRecord().getSchemaClass().getName(), "SubEdge");
  }
}

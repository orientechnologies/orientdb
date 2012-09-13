package com.orientechnologies.orient.graph.gremlin;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

public class BlueprintsTest {
  private static String DB_URL = "local:target/databases/tinkerpop";
  private OrientGraph   graph;

  public BlueprintsTest() {
  }

  @BeforeClass
  public void before() {
    graph = new OrientGraph(DB_URL);
    graph.getRawGraph().setUseCustomTypes(true);
  }

  @AfterClass
  public void after() {
    graph.shutdown();
  }

  @Test
  public void testSubVertex() {
    if (graph.getRawGraph().getVertexType("SubVertex") == null)
      graph.getRawGraph().createVertexType("SubVertex");

    Vertex v = graph.addVertex("class:SubVertex");
    v.setProperty("key", "subtype");
  }

  @Test
  public void testSubEdge() {
    if (graph.getRawGraph().getEdgeType("SubEdge") == null)
      graph.getRawGraph().createEdgeType("SubEdge");

    Vertex v1 = graph.addVertex("class:SubVertex");
    v1.setProperty("key", "subtype+subedge");

    Vertex v2 = graph.addVertex("class:SubVertex");
    v2.setProperty("key", "subtype+subedge");

    Edge e = graph.addEdge("class:SubEdge", v1, v2, null);
    e.setProperty("key", "subedge");
  }

//  @Test
//  public void testIndexAgainstList() {
//    graph.dropKeyIndex("list", Vertex.class);
//    graph.createKeyIndex("list", Vertex.class, new Parameter("type", "EMBEDDEDLIST"), new Parameter("embeddedType", "INTEGER"));
//
//    Vertex v1 = graph.addVertex(null);
//
//    List<Integer> list = new ArrayList<Integer>();
//    list.add(1);
//    list.add(2);
//    list.add(3);
//
//    v1.setProperty("list", list);
//
//    Iterable<Vertex> item = graph.getVertices("list", 1);
//    Assert.assertTrue(item.iterator().hasNext());
//
//    graph.dropKeyIndex("list", Vertex.class);
//  }
}

package com.orientechnologies.orient.graph.blueprints;

import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class GraphFunctionsTest {
  private static String DB_URL = "memory:" + GraphFunctionsTest.class.getSimpleName();
  private static OrientGraph graph;

  private static Vertex v1;
  private static Vertex v2;
  private static Vertex v3;
  private static Edge e1;
  private static Edge e2;

  public GraphFunctionsTest() {}

  @BeforeClass
  public static void before() {
    graph = new OrientGraph(DB_URL);

    if (graph.getEdgeType("SubEdge") == null) graph.createEdgeType("SubEdge");
    if (graph.getVertexType("SubVertex") == null) graph.createVertexType("SubVertex");

    v1 = graph.addVertex("class:SubVertex");
    v2 = graph.addVertex("class:SubVertex");
    v3 = graph.addVertex(null);

    e1 = graph.addEdge("class:SubEdge", v1, v2, null);
    e2 = graph.addEdge(null, v1, v3, "contains");
    graph.commit();
  }

  @AfterClass
  public static void after() {
    graph.drop();
    graph = null;
    v1 = null;
    v2 = null;
    v3 = null;

    e1 = null;
    e2 = null;
  }

  @Test
  public void testOut() {
    long found;

    // V1
    found = graph.sqlQuery("select expand( out() ) from " + v1.getId()).stream().count();
    Assert.assertEquals(found, 2);

    found = graph.sqlQuery("select expand( out('SubEdge') ) from " + v1.getId()).stream().count();
    Assert.assertEquals(found, 1);

    found = graph.sqlQuery("select expand( out('dddd') ) from " + v1.getId()).stream().count();
    Assert.assertEquals(found, 0);

    // V2
    found = graph.sqlQuery("select expand( out() ) from " + v2.getId()).stream().count();
    Assert.assertEquals(found, 0);
    // V3
    found = graph.sqlQuery("select expand( out() ) from " + v3.getId()).stream().count();
    Assert.assertEquals(found, 0);
  }

  @Test
  public void testIn() {
    long found;

    // V1

    found = graph.sqlQuery("select expand( in() ) from " + v1.getId()).stream().count();
    Assert.assertEquals(found, 0);

    // V2
    found = graph.sqlQuery("select expand( in() ) from " + v2.getId()).stream().count();
    Assert.assertEquals(found, 1);

    found = graph.sqlQuery("select expand( in('SubEdge') ) from " + v2.getId()).stream().count();
    Assert.assertEquals(found, 1);

    found = graph.sqlQuery("select expand( in('dddd') ) from " + v2.getId()).stream().count();
    Assert.assertEquals(found, 0);

    // V3
    found = graph.sqlQuery("select expand( in() ) from " + v3.getId()).stream().count();
    Assert.assertEquals(found, 1);
  }

  @Test
  public void testOutE() {
    long found;

    // V1
    found = graph.sqlQuery("select expand( outE() ) from " + v1.getId()).stream().count();
    Assert.assertEquals(found, 2);

    found = graph.sqlQuery("select expand( outE('SubEdge') ) from " + v1.getId()).stream().count();
    Assert.assertEquals(found, 1);

    found = graph.sqlQuery("select expand( outE('dddd') ) from " + v1.getId()).stream().count();
    Assert.assertEquals(found, 0);

    // V2
    found = graph.sqlQuery("select expand( outE() ) from " + v2.getId()).stream().count();
    Assert.assertEquals(found, 0);
    // V3
    found = graph.sqlQuery("select expand( outE() ) from " + v3.getId()).stream().count();
    Assert.assertEquals(found, 0);
  }

  @Test
  public void testInE() {
    long found;

    // V1
    found = graph.sqlQuery("select expand( inE() ) from " + v1.getId()).stream().count();
    Assert.assertEquals(found, 0);

    // V2
    found = graph.sqlQuery("select expand( inE() ) from " + v2.getId()).stream().count();
    Assert.assertEquals(found, 1);

    found = graph.sqlQuery("select expand( inE('SubEdge') ) from " + v2.getId()).stream().count();
    Assert.assertEquals(found, 1);

    found = graph.sqlQuery("select expand( inE('dddd') ) from " + v2.getId()).stream().count();
    Assert.assertEquals(found, 0);

    // V3
    found = graph.sqlQuery("select expand( inE() ) from " + v3.getId()).stream().count();
    Assert.assertEquals(found, 1);
  }

  @Test
  public void testOutV() {
    Iterable<Vertex> vertices;

    // V1
    vertices =
        (Iterable<Vertex>)
            graph
                .command(new OCommandSQL("select expand( outE().outV() ) from " + v1.getId()))
                .execute();
    Assert.assertEquals(vertices.iterator().next(), v1);

    vertices =
        (Iterable<Vertex>)
            graph
                .command(new OCommandSQL("select expand( outE().inV() ) from " + v1.getId()))
                .execute();
    Assert.assertEquals(vertices.iterator().next(), v2);

    // V2
    vertices =
        (Iterable<Vertex>)
            graph
                .command(new OCommandSQL("select expand( inE().inV() ) from " + v2.getId()))
                .execute();
    Assert.assertEquals(vertices.iterator().next(), v2);

    vertices =
        (Iterable<Vertex>)
            graph
                .command(new OCommandSQL("select expand( inE().outV() ) from " + v2.getId()))
                .execute();
    Assert.assertEquals(vertices.iterator().next(), v1);
  }

  @Test
  public void testOutEPolymorphic() {
    long found;

    // V1
    found = graph.sqlQuery("select expand( outE('E') ) from " + v1.getId()).stream().count();
    Assert.assertEquals(found, 2);
  }
}

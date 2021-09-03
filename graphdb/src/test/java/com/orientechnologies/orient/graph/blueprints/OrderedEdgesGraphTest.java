package com.orientechnologies.orient.graph.blueprints;

import static junit.framework.TestCase.assertEquals;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import java.util.List;
import org.junit.Test;

public class OrderedEdgesGraphTest {
  private static String DB_URL = "memory:" + OrderedEdgesGraphTest.class.getSimpleName();
  private static OrientGraph graph;
  private final OrientVertex mainPerson;

  public OrderedEdgesGraphTest() {
    graph = new OrientGraph(DB_URL);
    graph.setUseLightweightEdges(true);
    graph.setAutoStartTx(false);
    graph.commit();

    if (graph.getEdgeType("Knows") == null) {
      OrientEdgeType knows = graph.createEdgeType("Knows");
      OrientVertexType person = graph.createVertexType("Person");

      person.createEdgeProperty(Direction.OUT, "Knows").setOrdered(true);
    }

    graph.setAutoStartTx(true);

    mainPerson = graph.addVertex("class:Person", new Object[] {"index", 0});

    for (int i = 1; i < 101; ++i) {
      final Vertex newVertex = graph.addVertex("class:Person", new Object[] {"index", i});
      mainPerson.addEdge("Knows", newVertex);
    }
  }

  @Test
  public void testEdgeOrder() {
    try {
      OrientVertex loadedPerson = graph.getVertex(mainPerson.getId());
      graph.setUseLightweightEdges(true);
      int i = 1;
      for (Edge e : loadedPerson.getEdges(Direction.OUT)) {
        assertEquals(e.getVertex(Direction.IN).<Object>getProperty("index"), i++);
      }
    } finally {
      graph.shutdown();
    }
  }

  @Test
  public void testReplacePosition() {
    OrientVertex loadedPerson;
    List<ODocument> edges;
    try {
      loadedPerson = graph.getVertex(mainPerson.getId());
      graph.setUseLightweightEdges(true);
      int i = 1;
      edges = loadedPerson.getRecord().field("out_Knows");

      ODocument edge10 = edges.remove(9);
      edges.add(edge10);
    } finally {
      graph.shutdown();
    }

    graph = new OrientGraph(DB_URL);
    try {
      graph.setUseLightweightEdges(true);
      loadedPerson = graph.getVertex(mainPerson.getId());
      edges = loadedPerson.getRecord().field("out_Knows");
      assertEquals(graph.getVertex(edges.get(9)).<Object>getProperty("index"), 11);
      assertEquals(graph.getVertex(edges.get(99)).<Object>getProperty("index"), 10);
    } finally {
      graph.shutdown();
    }
  }
}

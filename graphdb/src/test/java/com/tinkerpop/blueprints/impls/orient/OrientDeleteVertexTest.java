package com.tinkerpop.blueprints.impls.orient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import java.util.Iterator;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OrientDeleteVertexTest {

  @After
  public void tearDown() {
    OrientGraph g = createGraph();
    g.drop();
  }

  @Test
  public void shouldDeleteEdgesWhenDeletingVertex() {

    // Create a node v1 with at least two edges
    OrientGraph g = createGraph();
    OrientVertex v1 = g.addVertex("class:V1");
    OrientVertex v2 = g.addVertex("class:V2");
    OrientVertex v22 = g.addVertex("class:V2");

    g.addEdge(null, v1, v2, "edgeType1");
    g.addEdge(null, v1, v22, "edgeType1");

    g.shutdown();

    // delete connected vertex v2
    g = createGraph();

    long total = g.countVertices();
    g.getVertex(v2.getId()).remove();

    assertEquals(total - 1, g.countVertices());

    // the v1 out_edgeType1 property should not contain a reference to
    // deleted node v2
    // OK INSIDE THE TRANSACTION
    Iterable<OrientEdge> out_edge = g.getVertex(v1.getId()).getProperty("out_edgeType1");

    boolean contains = false;
    for (OIdentifiable id : out_edge) if (id.equals(v2.getId())) contains = true;

    assertFalse(contains);
    g.shutdown();

    // the v1 node should only have one edge left
    // OK
    assertEquals(1, getEdgeCount(v1.getId()));

    g = createGraph();
    // v2 vertex sould be deleted
    // OK
    assertNull(g.getVertex(v2.getId()));

    // the v1 out_edgeType1 property should not contain a reference to
    // deleted v2
    // FAILS HERE OUTSIDE OF THE TRANSACTION
    out_edge = g.getVertex(v1.getId()).getProperty("out_edgeType1");

    contains = false;
    for (OIdentifiable id : out_edge) if (id.equals(v2.getId())) contains = true;

    assertFalse(contains);
    g.shutdown();
  }

  int getEdgeCount(Object vid) {
    OrientGraph g = createGraph();
    int n = 0;
    for (Iterator<Edge> it = g.getVertex(vid).getEdges(Direction.BOTH).iterator();
        it.hasNext();
        n++) {
      it.next();
    }
    g.shutdown();
    return n;
  }

  private OrientGraph createGraph() {
    return (OrientGraph)
        new OrientGraph("memory:" + OrientDeleteVertexTest.class.getSimpleName())
            .setWarnOnForceClosingTx(false);
  }
}

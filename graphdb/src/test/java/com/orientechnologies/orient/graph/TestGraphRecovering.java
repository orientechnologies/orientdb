package com.orientechnologies.orient.graph;

import org.junit.Assert;
import org.junit.Test;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OStorageRecoverEventListener;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OGraphRepair;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class TestGraphRecovering {

  private class TestListener implements OStorageRecoverEventListener {
    public long scannedEdges     = 0;
    public long removedEdges     = 0;
    public long scannedVertices  = 0;
    public long scannedLinks     = 0;
    public long removedLinks     = 0;
    public long repairedVertices = 0;

    @Override
    public void onScannedEdge(ODocument edge) {
      scannedEdges++;
    }

    @Override
    public void onRemovedEdge(ODocument edge) {
      removedEdges++;
    }

    @Override
    public void onScannedVertex(ODocument vertex) {
      scannedVertices++;
    }

    @Override
    public void onScannedLink(OIdentifiable link) {
      scannedLinks++;
    }

    @Override
    public void onRemovedLink(OIdentifiable link) {
      removedLinks++;
    }

    @Override
    public void onRepairedVertex(ODocument vertex) {
      repairedVertices++;
    }
  };

  private void init(OrientBaseGraph g) {
    g.createVertexType("V1");
    g.createVertexType("V2");
    g.createEdgeType("E1");
    g.createEdgeType("E2");

    final OrientVertex v0 = g.addVertex(null);
    final OrientVertex v1 = g.addVertex("class:V1");
    final OrientVertex v2 = g.addVertex("class:V2");

    v0.addEdge("E", v1);
    v1.addEdge("E1", v2);
    v2.addEdge("E2", v0);
  }

  @Test
  public void testRecoverPerfectGraph() {
    final OrientBaseGraph g = new OrientGraphNoTx("memory:testRecoverPerfectGraph");
    try {
      init(g);

      final TestListener eventListener = new TestListener();

      new OGraphRepair().setEventListener(eventListener).repair(g, null);

      Assert.assertEquals(eventListener.scannedEdges, 3);
      Assert.assertEquals(eventListener.removedEdges, 0);
      Assert.assertEquals(eventListener.scannedVertices, 3);
      Assert.assertEquals(eventListener.scannedLinks, 6);
      Assert.assertEquals(eventListener.removedLinks, 0);
      Assert.assertEquals(eventListener.repairedVertices, 0);

    } finally {
      g.shutdown();
    }
  }

  @Test
  public void testRecoverBrokenGraphAllEdges() {
    final OrientBaseGraph g = new OrientGraphNoTx("memory:testRecoverBrokenGraphAllEdges");
    try {
      init(g);

      for (Edge e : g.getEdges()) {
        ((OrientEdge) e).getRecord().removeField("out");
        ((OrientEdge) e).getRecord().save();
      }

      final TestListener eventListener = new TestListener();

      new OGraphRepair().setEventListener(eventListener).repair(g, null);

      Assert.assertEquals(eventListener.scannedEdges, 3);
      Assert.assertEquals(eventListener.removedEdges, 3);
      Assert.assertEquals(eventListener.scannedVertices, 3);
      Assert.assertEquals(eventListener.scannedLinks, 6);
      Assert.assertEquals(eventListener.removedLinks, 6);
      Assert.assertEquals(eventListener.repairedVertices, 3);

    } finally {
      g.shutdown();
    }
  }

  @Test
  public void testRecoverBrokenGraphLinksInVertices() {
    final OrientBaseGraph g = new OrientGraphNoTx("memory:testRecoverBrokenGraphLinksInVertices");
    try {
      init(g);

      for (Vertex v : g.getVertices()) {
        for (String f : ((OrientVertex) v).getRecord().fieldNames()) {
          if (f.startsWith("out_"))
            ((OrientVertex) v).getRecord().removeField(f);
        }
      }

      final TestListener eventListener = new TestListener();

      new OGraphRepair().setEventListener(eventListener).repair(g, null);

      Assert.assertEquals(eventListener.scannedEdges, 3);
      Assert.assertEquals(eventListener.removedEdges, 3);
      Assert.assertEquals(eventListener.scannedVertices, 3);
      Assert.assertEquals(eventListener.scannedLinks, 3);
      Assert.assertEquals(eventListener.removedLinks, 3);
      Assert.assertEquals(eventListener.repairedVertices, 3);

    } finally {
      g.shutdown();
    }
  }
}

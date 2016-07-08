package com.orientechnologies.orient.graph;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by tglman on 08/07/16.
 */
public class TestGraphIntentMassiveInsert {

  @Test
  public void testIntent() {
    final OrientGraph graph = new OrientGraph("memory:default", false);
    graph.setUseLightweightEdges(true);
    graph.getRawGraph().declareIntent(new OIntentMassiveInsert());
    final OrientVertexType c1 = graph.createVertexType("C1");
    c1.createProperty("p1", OType.INTEGER);
    graph.createEdgeType("parent");
    graph.begin();
    final OrientVertex first = graph.addVertex("class:C1");
    first.setProperty("p1", -1);
    for (int i = 0; i < 10; i++) {
      final OrientVertex v = graph.addVertex("class:C1");
      v.setProperty("p1", i);
      first.addEdge("parent", v);
      //this search fills _source
      graph.command(new OSQLSynchQuery("SELECT from V where p1='" + i + "'")).execute();
    }
    graph.commit();
    //here NPE will be thrown
    final Iterable<Edge> edges = first.getEdges(Direction.BOTH);
    Iterator<Edge> ter = edges.iterator();
    for (int i = 0; i < 10; i++) {
      assertTrue(ter.hasNext());
      assertEquals(ter.next().getVertex(Direction.IN).getProperty("p1"), (Integer) i);
    }
  }

}

package com.tinkerpop.blueprints.impls.orient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import java.util.Iterator;
import org.junit.Test;

public class SelectProjectionVertexTest {

  @Test
  public void test() {
    OrientGraph graph =
        new OrientGraph("memory:" + SelectProjectionVertexTest.class.getSimpleName());
    try {
      graph.createVertexType("VertA");
      graph.createVertexType("VertB");
      graph.createEdgeType("AtoB");
      OrientVertex root = graph.addVertex("class:VertA");
      graph.commit();

      for (int i = 0; i < 2; i++) {
        OrientVertex v = graph.addVertex("class:VertB");
        root.addEdge("AtoB", v);
      }
      graph.commit();

      String query =
          "SELECT $res as val LET $res = (SELECT @rid AS refId, out('AtoB') AS vertices FROM VertA) FETCHPLAN val:2";

      Iterable<OrientVertex> results = graph.command(new OCommandSQL(query)).execute();
      final Iterator<OrientVertex> iterator = results.iterator();
      assertTrue(iterator.hasNext());
      OrientVertex result = iterator.next();

      Iterable<OrientVertex> vertices = result.getProperty("val");
      for (OrientVertex vertex : vertices) {
        assertEquals(
            ((OIdentifiable) vertex.getProperty("refId")).getIdentity(), root.getIdentity());
      }
    } finally {
      graph.drop();
    }
  }
}

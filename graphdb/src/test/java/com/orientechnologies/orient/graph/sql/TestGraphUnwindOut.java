package com.orientechnologies.orient.graph.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.impls.orient.OrientElement;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

public class TestGraphUnwindOut {

  @Test
  public void testUwindLightweightEdges() {
    OrientGraph graph = new OrientGraph("memory:" + TestGraphUnwindOut.class.getSimpleName());
    graph.setUseLightweightEdges(true);
    try {

      OrientVertexType type = graph.createVertexType("edgetest");
      graph.createEdgeType("edgetestedge");
      type.createEdgeProperty(Direction.IN, "edgetestedge");
      type.createEdgeProperty(Direction.OUT, "edgetestedge");

      OrientVertex test = graph.addVertex("class:edgetest");
      test.setProperty("ida", "parentckt1");
      test.save();
      OrientVertex test1 = graph.addVertex("class:edgetest");
      test1.setProperty("ida", "childckt2");
      test1.save();
      OrientVertex test2 = graph.addVertex("class:edgetest");
      test2.setProperty("ida", "childckt3");
      test2.save();
      OrientVertex test3 = graph.addVertex("class:edgetest");
      test3.setProperty("ida", "childckt4");
      test3.save();
      graph.commit();
      graph
          .command(new OCommandSQL(
              "create edge edgetestedge from (select from edgetest where ida='parentckt1') to (select from edgetest where ida like 'childckt%')"))
          .execute();
      graph.commit();
      Iterable<OrientElement> res = graph
          .command(
              new OSQLSynchQuery("select out_edgetestedge[0] from v where out_edgetestedge.size() > 0 unwind out_edgetestedge "))
          .execute();

      for (OrientElement oDocument : res) {
        assertNotNull(oDocument.getRecord().field("out_edgetestedge"));
        ODocument doc = oDocument.getRecord().field("out_edgetestedge");
        assertEquals(doc.getClassName(), "edgetest");
      }

    } finally {
      graph.drop();
    }
  }

}

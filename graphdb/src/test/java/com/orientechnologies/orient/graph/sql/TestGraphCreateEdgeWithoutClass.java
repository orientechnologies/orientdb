package com.orientechnologies.orient.graph.sql;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.impls.orient.OrientElement;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestGraphCreateEdgeWithoutClass {

  @Test
  public void testCreateEdgeWithoutClass() {
    OrientGraph graph = new OrientGraph("memory:" + TestGraphCreateEdgeWithoutClass.class.getSimpleName());
    graph.setUseClassForEdgeLabel(false);
    graph.setUseVertexFieldsForEdgeLabels(true);
    try {



      OrientVertex test = graph.addVertex("class:V");
      test.setProperty("name", "foo");
      test.save();
      OrientVertex test1 = graph.addVertex("class:V");
      test1.setProperty("name", "bar");
      test1.save();
      graph.commit();
      graph
          .command(new OCommandSQL(
              "create edge FooBar from (select from V where name='foo') to (select from V where name = 'bar')"))
          .execute();
      graph.commit();

      Iterable<OrientElement> res = graph
          .command(
              new OSQLSynchQuery("select out as f1, out_ as f2, out_edgetestedge as f3, out_FooBar as f4, outE() as f5 from v where name = 'foo'"))
          .execute();


      boolean found = false;
      for (OrientElement oDocument : res) {
        assertNull(oDocument.getRecord().field("f1"));
        assertNull(oDocument.getRecord().field("f2"));
        assertNull(oDocument.getRecord().field("f3"));
        assertNotNull(oDocument.getRecord().field("f4"));
        Iterable<ODocument> f5 = oDocument.getRecord().field("f5");
        assertNotNull(f5);
        for(ODocument e:f5) {
          assertEquals(e.field("label"), "FooBar");
          found = true;
        }
      }
      assertTrue(found);

    } finally {
      graph.drop();
    }
  }

}

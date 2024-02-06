package com.orientechnologies.orient.graph.sql;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DeleteEdgeTest {

  private OrientGraph graph;
  private OrientEdgeType edgeType;

  @Before
  public void before() {
    graph = new OrientGraph("memory:" + DeleteEdgeTest.class.getSimpleName(), "admin", "admin");
    graph.createVertexType("TestVertex");
    edgeType = graph.createEdgeType("TestEdge");
  }

  @After
  public void after() {
    graph.drop();
  }

  @Test
  public void testDeleteEdge() {

    for (int i = 0; i < 10; i++) {
      OrientVertex v1 = graph.addVertex("class:TestVertex");
      OrientVertex v2 = graph.addVertex("class:TestVertex");
      OrientVertex v3 = graph.addVertex("class:TestVertex");
      OrientVertex v4 = graph.addVertex("class:TestVertex");

      Map<String, Object> p1 = new HashMap<String, Object>();
      p1.put("based_on", "0001");
      OrientEdge e1 = v1.addEdge(null, v2, "TestEdge", null, p1);
      e1.save();

      Map<String, Object> p2 = new HashMap<String, Object>();
      p2.put("based_on", "0002");
      OrientEdge e2 = v3.addEdge(null, v4, "TestEdge", null, p2);
      e2.save();

      graph.commit();

      graph.sqlCommand("delete edge TestEdge where based_on = '0001'").close();

      Iterable<OrientVertex> edges =
          graph
              .command(new OCommandSQL("select count(*) from TestEdge where based_on = '0001'"))
              .execute();
      assertTrue(edges.iterator().hasNext());
      assertEquals(edges.iterator().next().<Object>getProperty("count"), 0l);
    }
  }

  @Test
  public void testDeleteEdgeValidation() {

    OrientVertex v1 = graph.addVertex("class:TestVertex");
    OrientVertex v2 = graph.addVertex("class:TestVertex");

    Map<String, Object> p1 = new HashMap<String, Object>();
    p1.put("based_on", "0001");
    OrientEdge e1 = v1.addEdge(null, v2, "TestEdge", null, p1);
    e1.save();

    graph.commit();
    graph.getRawGraph().commit();
    edgeType.createProperty("mand", OType.STRING).setMandatory(true);
    graph.getRawGraph().begin();

    graph.sqlCommand("delete edge TestEdge where based_on = '0001'").close();

    Iterable<OrientVertex> edges =
        graph
            .command(new OCommandSQL("select count(*) from TestEdge where based_on = '0001'"))
            .execute();
    assertTrue(edges.iterator().hasNext());
    assertEquals(edges.iterator().next().<Object>getProperty("count"), 0l);
  }
}

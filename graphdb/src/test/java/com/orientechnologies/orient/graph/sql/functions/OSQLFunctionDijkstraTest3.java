package com.orientechnologies.orient.graph.sql.functions;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.id.ORID;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class OSQLFunctionDijkstraTest3 {

  private static OrientGraph          graph;
  private static OrientVertex         v1;
  private static OrientVertex         v2;
  private static OrientVertex         v3;
  private static OrientVertex         v4;
  private static OSQLFunctionDijkstra3 functionDijkstra;

  @BeforeClass
  public static void setUp() throws Exception {
    setUpDatabase();

    functionDijkstra = new OSQLFunctionDijkstra3();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    graph.shutdown();
  }

  private static void setUpDatabase() {
    graph = new OrientGraph("memory:OSQLFunctionDijkstraTest");
		graph.createEdgeType("weight");
		graph.createEdgeType("other");

    v1 = graph.addVertex(null);
    v2 = graph.addVertex(null);
    v3 = graph.addVertex(null);
    v4 = graph.addVertex(null);

    v1.setProperty("node_id", "A");
    v2.setProperty("node_id", "B");
    v3.setProperty("node_id", "C");
    v4.setProperty("node_id", "D");

    Edge e1 = graph.addEdge(null, v1, v2, "weight");
    e1.setProperty("weight", 1.0f);

    Edge e2 = graph.addEdge(null, v2, v3, "weight");
    e2.setProperty("weight", 1.0f);
    Edge e3 = graph.addEdge(null, v1, v3, "weight");
    e3.setProperty("weight", 100.0f);
    Edge e4 = graph.addEdge(null, v3, v4, "weight");
    e4.setProperty("weight", 1.0f);
    Edge e5 = graph.addEdge(null, v2, v4, "weight"); // e5 has no "weight" property
    Edge e6 = graph.addEdge(null, v1, v4, "other");
    e6.setProperty("weight", 0.1f);
    Edge e7 = graph.addEdge(null, v1, v4, "other");
    e7.setProperty("weight", 1.0f);
    graph.commit();
  }

  @Test
  public void testExecute() throws Exception {
    final List<ORID> result = functionDijkstra.execute(null, null, null, new Object[] { v1, v4,"'weight'", "OUT", "weight" },
        new OBasicCommandContext());

    assertEquals(4, result.size());
    assertEquals(v1.getIdentity(), result.get(0));
    assertEquals(v2.getIdentity(), result.get(1));
    assertEquals(v3.getIdentity(), result.get(2));
    assertEquals(v4.getIdentity(), result.get(3));
  }

  @Test
  public void testExecute2() throws Exception {
    final List<ORID> result = functionDijkstra.execute(null, null, null, new Object[] { v1, v4,"'weight'", "OUT", "other" },
        new OBasicCommandContext());

    assertEquals(2, result.size());
    assertEquals(v1.getIdentity(), result.get(0));
    assertEquals(v4.getIdentity(), result.get(1));
  }

  @Test
  public void testExecute3() throws Exception {
    final List<ORID> result = functionDijkstra.execute(null, null, null, new Object[] { v1, v3,"'weight'", "OUT", "other" },
        new OBasicCommandContext());

    assertEquals(0, result.size());
  }
  
  @Test
  public void testExecute4() throws Exception {
    final List<ORID> result = functionDijkstra.execute(null, null, null, new Object[] { v1, v1,"'weight'", "OUT", "weight" },
        new OBasicCommandContext());

    assertEquals(1, result.size());
    assertEquals(v1.getIdentity(), result.get(0));
  }
  
  @Test
  public void testExecute5() throws Exception {
    final List<ORID> result = functionDijkstra.execute(null, null, null, new Object[] { v4, v1, "'weight'", "OUT", "weight" },
        new OBasicCommandContext());

    assertEquals(0, result.size());
  }
  
  @Test
  public void testExecute6() throws Exception {
    final List<ORID> result = functionDijkstra.execute(null, null, null, new Object[] { v4, v1,"'weight'", "BOTH", "weight" },
        new OBasicCommandContext());

    assertEquals(4, result.size());
    assertEquals(v4.getIdentity(), result.get(0));
    assertEquals(v3.getIdentity(), result.get(1));
    assertEquals(v2.getIdentity(), result.get(2));
    assertEquals(v1.getIdentity(), result.get(3));
  }
  
  @Test
  public void testExecute7() throws Exception {
    final List<ORID> result = functionDijkstra.execute(null, null, null, new Object[] { v4, v1,"'weight'", "IN", "weight" },
        new OBasicCommandContext());

    assertEquals(4, result.size());
    assertEquals(v4.getIdentity(), result.get(0));
    assertEquals(v3.getIdentity(), result.get(1));
    assertEquals(v2.getIdentity(), result.get(2));
    assertEquals(v1.getIdentity(), result.get(3));
  }
}

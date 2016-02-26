package com.orientechnologies.orient.graph.sql.functions;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.id.ORID;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class OSQLFunctionShortestPathTest {

  private OrientGraph graph;
  private Map<Integer, Vertex> vertices = new HashMap<Integer, Vertex>();

  private OSQLFunctionShortestPath function;

  @Before
  public void setUp() throws Exception {
    setUpDatabase();

    function = new OSQLFunctionShortestPath();
  }

  @After
  public void tearDown() throws Exception {
    graph.shutdown();
  }

  private void setUpDatabase() {
    graph = new OrientGraph("memory:OSQLFunctionShortestPath");

    vertices.put(1, graph.addVertex(null));
    vertices.put(2, graph.addVertex(null));
    vertices.put(3, graph.addVertex(null));
    vertices.put(4, graph.addVertex(null));

    vertices.get(1).setProperty("node_id", "A");
    vertices.get(2).setProperty("node_id", "B");
    vertices.get(3).setProperty("node_id", "C");
    vertices.get(4).setProperty("node_id", "D");

    graph.addEdge(null, vertices.get(1), vertices.get(2), "Edge1");
    graph.addEdge(null, vertices.get(2), vertices.get(3), "Edge1");
    graph.addEdge(null, vertices.get(3), vertices.get(1), "Edge2");
    graph.addEdge(null, vertices.get(3), vertices.get(4), "Edge1");

    for (int i = 5; i <= 20; i++) {
      vertices.put(i, graph.addVertex(null));
      vertices.get(i).setProperty("node_id", "V" + i);
      graph.addEdge(null, vertices.get(i - 1), vertices.get(i), "Edge1");
      if (i % 2 == 0) {
        graph.addEdge(null, vertices.get(i - 2), vertices.get(i), "Edge1");
      }
    }
    graph.commit();
  }

  @Test
  public void testExecute() throws Exception {
    final List<ORID> result = function.execute(null, null, null, new Object[] { vertices.get(1), vertices.get(4) },
        new OBasicCommandContext());

    assertEquals(3, result.size());
    assertEquals(vertices.get(1).getId(), result.get(0));
    assertEquals(vertices.get(3).getId(), result.get(1));
    assertEquals(vertices.get(4).getId(), result.get(2));
  }

  @Test
  public void testExecuteOut() throws Exception {
    final List<ORID> result = function.execute(null, null, null, new Object[] { vertices.get(1), vertices.get(4), "out", null },
        new OBasicCommandContext());

    assertEquals(4, result.size());
    assertEquals(vertices.get(1).getId(), result.get(0));
    assertEquals(vertices.get(2).getId(), result.get(1));
    assertEquals(vertices.get(3).getId(), result.get(2));
    assertEquals(vertices.get(4).getId(), result.get(3));
  }

  @Test
  public void testExecuteOnlyEdge1() throws Exception {
    final List<ORID> result = function.execute(null, null, null, new Object[] { vertices.get(1), vertices.get(4), null, "Edge1" },
        new OBasicCommandContext());

    assertEquals(4, result.size());
    assertEquals(vertices.get(1).getId(), result.get(0));
    assertEquals(vertices.get(2).getId(), result.get(1));
    assertEquals(vertices.get(3).getId(), result.get(2));
    assertEquals(vertices.get(4).getId(), result.get(3));
  }

  @Test
  public void testLong() throws Exception {
    final List<ORID> result = function.execute(null, null, null, new Object[] { vertices.get(1), vertices.get(20) },
        new OBasicCommandContext());

    assertEquals(11, result.size());
    assertEquals(vertices.get(1).getId(), result.get(0));
    assertEquals(vertices.get(3).getId(), result.get(1));
    int next = 2;
    for (int i = 4; i <= 20; i += 2) {
      assertEquals(vertices.get(i).getId(), result.get(next++));
    }
  }

  @Test
  public void testMaxDepth1() throws Exception {
    Map<String, Object> additionalParams = new HashMap<String, Object>();
    additionalParams.put(OSQLFunctionShortestPath.PARAM_MAX_DEPTH, 11);
    final List<ORID> result = function
        .execute(null, null, null, new Object[] { vertices.get(1), vertices.get(20), null, null, additionalParams },
            new OBasicCommandContext());

    assertEquals(11, result.size());
  }

  @Test
  public void testMaxDepth2() throws Exception {
    Map<String, Object> additionalParams = new HashMap<String, Object>();
    additionalParams.put(OSQLFunctionShortestPath.PARAM_MAX_DEPTH, 12);
    final List<ORID> result = function
        .execute(null, null, null, new Object[] { vertices.get(1), vertices.get(20), null, null, additionalParams },
            new OBasicCommandContext());

    assertEquals(11, result.size());
  }

  @Test
  public void testMaxDepth3() throws Exception {
    Map<String, Object> additionalParams = new HashMap<String, Object>();
    additionalParams.put(OSQLFunctionShortestPath.PARAM_MAX_DEPTH, 10);
    final List<ORID> result = function
        .execute(null, null, null, new Object[] { vertices.get(1), vertices.get(20), null, null, additionalParams },
            new OBasicCommandContext());

    assertEquals(0, result.size());
  }

  @Test
  public void testMaxDepth4() throws Exception {
    Map<String, Object> additionalParams = new HashMap<String, Object>();
    additionalParams.put(OSQLFunctionShortestPath.PARAM_MAX_DEPTH, 3);
    final List<ORID> result = function
        .execute(null, null, null, new Object[] { vertices.get(1), vertices.get(20), null, null, additionalParams },
            new OBasicCommandContext());

    assertEquals(0, result.size());
  }
}

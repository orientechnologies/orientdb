/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.graph.sql.functions;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/*
 * @author Saeed Tabrizi (saeed a_t  nowcando.com)
 */
public class OSQLFunctionAstarTest {
  private static int dbCounter = 0;
  private OrientGraph graph;
  private Vertex v0;
  private Vertex v1;
  private Vertex v2;
  private Vertex v3;
  private Vertex v4;
  private Vertex v5;
  private Vertex v6;
  private OSQLFunctionAstar functionAstar;

  @Before
  public void setUp() throws Exception {

    setUpDatabase();

    functionAstar = new OSQLFunctionAstar();
  }

  @After
  public void tearDown() throws Exception {
    graph.shutdown();
  }

  private void setUpDatabase() {
    dbCounter++;
    graph = new OrientGraph("memory:OSQLFunctionAstarTest" + dbCounter);
    graph.createEdgeType("has_path");
    OFunction cf =
        graph.getRawGraph().getMetadata().getFunctionLibrary().createFunction("myCustomHeuristic");
    cf.setCode("return 1;");

    v0 = graph.addVertex(null);
    v1 = graph.addVertex(null);
    v2 = graph.addVertex(null);
    v3 = graph.addVertex(null);
    v4 = graph.addVertex(null);
    v5 = graph.addVertex(null);
    v6 = graph.addVertex(null);

    v0.setProperty("node_id", "Z"); // Tabriz
    v0.setProperty("name", "Tabriz");
    v0.setProperty("lat", 31.746512f);
    v0.setProperty("lon", 51.427002f);
    v0.setProperty("alt", 2200);

    v1.setProperty("node_id", "A"); // Tehran
    v1.setProperty("name", "Tehran");
    v1.setProperty("lat", 35.746512f);
    v1.setProperty("lon", 51.427002f);
    v1.setProperty("alt", 1800);

    v2.setProperty("node_id", "B"); // Mecca
    v2.setProperty("name", "Mecca");
    v2.setProperty("lat", 21.371244f);
    v2.setProperty("lon", 39.847412f);
    v2.setProperty("alt", 1500);

    v3.setProperty("node_id", "C"); // Bejin
    v3.setProperty("name", "Bejin");
    v3.setProperty("lat", 39.904041f);
    v3.setProperty("lon", 116.408011f);
    v3.setProperty("alt", 1200);

    v4.setProperty("node_id", "D"); // London
    v4.setProperty("name", "London");
    v4.setProperty("lat", 51.495065f);
    v4.setProperty("lon", -0.120850f);
    v4.setProperty("alt", 900);

    v5.setProperty("node_id", "E"); // NewYork
    v5.setProperty("name", "NewYork");
    v5.setProperty("lat", 42.779275f);
    v5.setProperty("lon", -74.641113f);
    v5.setProperty("alt", 1700);

    v6.setProperty("node_id", "F"); // Los Angles
    v6.setProperty("name", "Los Angles");
    v6.setProperty("lat", 34.052234f);
    v6.setProperty("lon", -118.243685f);
    v6.setProperty("alt", 400);

    Edge e1 = graph.addEdge(null, v1, v2, "has_path");
    e1.setProperty("weight", 250.0f);
    e1.setProperty("ptype", "road");
    Edge e2 = graph.addEdge(null, v2, v3, "has_path");
    e2.setProperty("weight", 250.0f);
    e2.setProperty("ptype", "road");
    Edge e3 = graph.addEdge(null, v1, v3, "has_path");
    e3.setProperty("weight", 1000.0f);
    e3.setProperty("ptype", "road");
    Edge e4 = graph.addEdge(null, v3, v4, "has_path");
    e4.setProperty("weight", 250.0f);
    e4.setProperty("ptype", "road");
    Edge e5 = graph.addEdge(null, v2, v4, "has_path");
    e5.setProperty("weight", 600.0f);
    e5.setProperty("ptype", "road");
    Edge e6 = graph.addEdge(null, v4, v5, "has_path");
    e6.setProperty("weight", 400.0f);
    e6.setProperty("ptype", "road");
    Edge e7 = graph.addEdge(null, v5, v6, "has_path");
    e7.setProperty("weight", 300.0f);
    e7.setProperty("ptype", "road");
    Edge e8 = graph.addEdge(null, v3, v6, "has_path");
    e8.setProperty("weight", 200.0f);
    e8.setProperty("ptype", "road");
    Edge e9 = graph.addEdge(null, v4, v6, "has_path");
    e9.setProperty("weight", 900.0f);
    e9.setProperty("ptype", "road");
    Edge e10 = graph.addEdge(null, v2, v6, "has_path");
    e10.setProperty("weight", 2500.0f);
    e10.setProperty("ptype", "road");
    Edge e11 = graph.addEdge(null, v1, v5, "has_path");
    e11.setProperty("weight", 100.0f);
    e11.setProperty("ptype", "road");
    Edge e12 = graph.addEdge(null, v4, v1, "has_path");
    e12.setProperty("weight", 200.0f);
    e12.setProperty("ptype", "road");
    Edge e13 = graph.addEdge(null, v5, v3, "has_path");
    e13.setProperty("weight", 800.0f);
    e13.setProperty("ptype", "road");
    Edge e14 = graph.addEdge(null, v5, v2, "has_path");
    e14.setProperty("weight", 500.0f);
    e14.setProperty("ptype", "road");
    Edge e15 = graph.addEdge(null, v6, v5, "has_path");
    e15.setProperty("weight", 250.0f);
    e15.setProperty("ptype", "road");
    Edge e16 = graph.addEdge(null, v3, v1, "has_path");
    e16.setProperty("weight", 550.0f);
    e16.setProperty("ptype", "road");
    graph.commit();
  }

  @Test
  public void test1Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(OSQLFunctionAstar.PARAM_DIRECTION, "out");
    options.put(OSQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(OSQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[] {"has_path"});
    final List<OrientVertex> result =
        functionAstar.execute(
            null,
            null,
            null,
            new Object[] {v1, v4, "'weight'", options},
            new OBasicCommandContext());
    assertEquals(16, graph.countEdges("has_path"));
    assertEquals(4, result.size());
    assertEquals(v1, result.get(0));
    assertEquals(v2, result.get(1));
    assertEquals(v3, result.get(2));
    assertEquals(v4, result.get(3));
  }

  @Test
  public void test2Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(OSQLFunctionAstar.PARAM_DIRECTION, "out");
    options.put(OSQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(OSQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[] {"has_path"});
    final List<OrientVertex> result =
        functionAstar.execute(
            null,
            null,
            null,
            new Object[] {v1, v6, "'weight'", options},
            new OBasicCommandContext());
    assertEquals(16, graph.countEdges("has_path"));
    assertEquals(3, result.size());
    assertEquals(v1, result.get(0));
    assertEquals(v5, result.get(1));
    assertEquals(v6, result.get(2));
  }

  @Test
  public void test3Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(OSQLFunctionAstar.PARAM_DIRECTION, "out");
    options.put(OSQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(OSQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[] {"has_path"});
    options.put(OSQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[] {"lat", "lon"});
    final List<OrientVertex> result =
        functionAstar.execute(
            null,
            null,
            null,
            new Object[] {v1, v6, "'weight'", options},
            new OBasicCommandContext());
    assertEquals(16, graph.countEdges("has_path"));
    assertEquals(3, result.size());
    assertEquals(v1, result.get(0));
    assertEquals(v5, result.get(1));
    assertEquals(v6, result.get(2));
  }

  @Test
  public void test4Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(OSQLFunctionAstar.PARAM_DIRECTION, "out");
    options.put(OSQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(OSQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[] {"has_path"});
    options.put(OSQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[] {"lat", "lon", "alt"});
    final List<OrientVertex> result =
        functionAstar.execute(
            null,
            null,
            null,
            new Object[] {v1, v6, "'weight'", options},
            new OBasicCommandContext());
    assertEquals(16, graph.countEdges("has_path"));
    assertEquals(3, result.size());
    assertEquals(v1, result.get(0));
    assertEquals(v5, result.get(1));
    assertEquals(v6, result.get(2));
  }

  @Test
  public void test5Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(OSQLFunctionAstar.PARAM_DIRECTION, "out");
    options.put(OSQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(OSQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[] {"has_path"});
    options.put(OSQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[] {"lat", "lon"});
    final List<OrientVertex> result =
        functionAstar.execute(
            null,
            null,
            null,
            new Object[] {v3, v5, "'weight'", options},
            new OBasicCommandContext());
    assertEquals(16, graph.countEdges("has_path"));
    assertEquals(3, result.size());
    assertEquals(v3, result.get(0));
    assertEquals(v6, result.get(1));
    assertEquals(v5, result.get(2));
  }

  @Test
  public void test6Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(OSQLFunctionAstar.PARAM_DIRECTION, "out");
    options.put(OSQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(OSQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[] {"has_path"});
    options.put(OSQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[] {"lat", "lon"});
    final List<OrientVertex> result =
        functionAstar.execute(
            null,
            null,
            null,
            new Object[] {v6, v1, "'weight'", options},
            new OBasicCommandContext());
    assertEquals(16, graph.countEdges("has_path"));
    assertEquals(6, result.size());
    assertEquals(v6, result.get(0));
    assertEquals(v5, result.get(1));
    assertEquals(v2, result.get(2));
    assertEquals(v3, result.get(3));
    assertEquals(v4, result.get(4));
    assertEquals(v1, result.get(5));
  }

  @Test
  public void test7Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(OSQLFunctionAstar.PARAM_DIRECTION, "out");
    options.put(OSQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(OSQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[] {"has_path"});
    options.put(OSQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[] {"lat", "lon"});
    options.put(OSQLFunctionAstar.PARAM_HEURISTIC_FORMULA, "EucliDEAN");
    final List<OrientVertex> result =
        functionAstar.execute(
            null,
            null,
            null,
            new Object[] {v6, v1, "'weight'", options},
            new OBasicCommandContext());
    assertEquals(16, graph.countEdges("has_path"));
    assertEquals(6, result.size());
    assertEquals(v6, result.get(0));
    assertEquals(v5, result.get(1));
    assertEquals(v2, result.get(2));
    assertEquals(v3, result.get(3));
    assertEquals(v4, result.get(4));
    assertEquals(v1, result.get(5));
  }

  @Test
  public void test8Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(OSQLFunctionAstar.PARAM_DIRECTION, Direction.OUT);
    options.put(OSQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(OSQLFunctionAstar.PARAM_TIE_BREAKER, false);
    options.put(OSQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[] {"has_path"});
    options.put(OSQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[] {"lat", "lon"});
    options.put(OSQLFunctionAstar.PARAM_HEURISTIC_FORMULA, HeuristicFormula.EUCLIDEANNOSQR);
    final List<OrientVertex> result =
        functionAstar.execute(
            null,
            null,
            null,
            new Object[] {v6, v1, "'weight'", options},
            new OBasicCommandContext());
    assertEquals(16, graph.countEdges("has_path"));
    assertEquals(5, result.size());
    assertEquals(v6, result.get(0));
    assertEquals(v5, result.get(1));
    assertEquals(v2, result.get(2));
    assertEquals(v4, result.get(3));
    assertEquals(v1, result.get(4));
  }

  @Test
  public void test9Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(OSQLFunctionAstar.PARAM_DIRECTION, Direction.BOTH);
    options.put(OSQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(OSQLFunctionAstar.PARAM_TIE_BREAKER, false);
    options.put(OSQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[] {"has_path"});
    options.put(OSQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[] {"lat", "lon"});
    options.put(OSQLFunctionAstar.PARAM_HEURISTIC_FORMULA, HeuristicFormula.MAXAXIS);
    final List<OrientVertex> result =
        functionAstar.execute(
            null,
            null,
            null,
            new Object[] {v6, v1, "'weight'", options},
            new OBasicCommandContext());
    assertEquals(16, graph.countEdges("has_path"));
    assertEquals(3, result.size());
    assertEquals(v6, result.get(0));
    assertEquals(v5, result.get(1));
    assertEquals(v1, result.get(2));
  }

  @Test
  public void test10Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(OSQLFunctionAstar.PARAM_DIRECTION, Direction.OUT);
    options.put(OSQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(OSQLFunctionAstar.PARAM_TIE_BREAKER, false);
    options.put(OSQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[] {"has_path"});
    options.put(OSQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[] {"lat", "lon"});
    options.put(OSQLFunctionAstar.PARAM_HEURISTIC_FORMULA, HeuristicFormula.CUSTOM);
    options.put(OSQLFunctionAstar.PARAM_CUSTOM_HEURISTIC_FORMULA, "myCustomHeuristic");
    final List<OrientVertex> result =
        functionAstar.execute(
            null,
            null,
            null,
            new Object[] {v6, v1, "'weight'", options},
            new OBasicCommandContext());
    assertEquals(16, graph.countEdges("has_path"));
    assertEquals(6, result.size());
    assertEquals(v6, result.get(0));
    assertEquals(v5, result.get(1));
    assertEquals(v2, result.get(2));
    assertEquals(v3, result.get(3));
    assertEquals(v4, result.get(4));
    assertEquals(v1, result.get(5));
  }

  @Test
  public void test11Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(OSQLFunctionAstar.PARAM_DIRECTION, Direction.OUT);
    options.put(OSQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(OSQLFunctionAstar.PARAM_TIE_BREAKER, false);
    options.put(OSQLFunctionAstar.PARAM_EMPTY_IF_MAX_DEPTH, true);
    options.put(OSQLFunctionAstar.PARAM_MAX_DEPTH, 3);
    options.put(OSQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[] {"has_path"});
    options.put(OSQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[] {"lat", "lon"});
    options.put(OSQLFunctionAstar.PARAM_HEURISTIC_FORMULA, HeuristicFormula.CUSTOM);
    options.put(OSQLFunctionAstar.PARAM_CUSTOM_HEURISTIC_FORMULA, "myCustomHeuristic");
    final List<OrientVertex> result =
        functionAstar.execute(
            null,
            null,
            null,
            new Object[] {v6, v1, "'weight'", options},
            new OBasicCommandContext());
    assertEquals(16, graph.countEdges("has_path"));
    assertEquals(0, result.size());
  }

  @Test
  public void test12Execute() throws Exception {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(OSQLFunctionAstar.PARAM_DIRECTION, Direction.OUT);
    options.put(OSQLFunctionAstar.PARAM_PARALLEL, true);
    options.put(OSQLFunctionAstar.PARAM_TIE_BREAKER, false);
    options.put(OSQLFunctionAstar.PARAM_EMPTY_IF_MAX_DEPTH, false);
    options.put(OSQLFunctionAstar.PARAM_MAX_DEPTH, 3);
    options.put(OSQLFunctionAstar.PARAM_EDGE_TYPE_NAMES, new String[] {"has_path"});
    options.put(OSQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES, new String[] {"lat", "lon"});
    options.put(OSQLFunctionAstar.PARAM_HEURISTIC_FORMULA, HeuristicFormula.CUSTOM);
    options.put(OSQLFunctionAstar.PARAM_CUSTOM_HEURISTIC_FORMULA, "myCustomHeuristic");
    final List<OrientVertex> result =
        functionAstar.execute(
            null,
            null,
            null,
            new Object[] {v6, v1, "'weight'", options},
            new OBasicCommandContext());
    assertEquals(16, graph.countEdges("has_path"));
    assertEquals(4, result.size());
    assertEquals(v6, result.get(0));
    assertEquals(v5, result.get(1));
    assertEquals(v2, result.get(2));
    assertEquals(v3, result.get(3));
  }

  @Test
  public void testSql() {
    Iterable r =
        graph
            .command(
                new OSQLSynchQuery(
                    "select expand(astar("
                        + v1.getId()
                        + ", "
                        + v4.getId()
                        + ", 'weight', {'direction':'out', 'parallel':true, 'edgeTypeNames':'has_path'}))"))
            .execute();

    List result = new ArrayList();
    for (Object x : r) {
      result.add(x);
    }
    assertEquals(16, graph.countEdges("has_path"));
    assertEquals(4, result.size());
    assertEquals(v1, result.get(0));
    assertEquals(v2, result.get(1));
    assertEquals(v3, result.get(2));
    assertEquals(v4, result.get(3));
  }
}

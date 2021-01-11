package com.orientechnologies.orient.core.sql.functions.graph;

import static java.util.Arrays.asList;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OSQLFunctionShortestPathTest {

  private OrientDB orientDB;
  private ODatabaseDocument graph;

  private Map<Integer, OVertex> vertices = new HashMap<Integer, OVertex>();
  private OSQLFunctionShortestPath function;

  @Before
  public void setUp() throws Exception {
    setUpDatabase();

    function = new OSQLFunctionShortestPath();
  }

  @After
  public void tearDown() throws Exception {
    graph.close();
    orientDB.close();
  }

  private void setUpDatabase() {
    orientDB =
        OCreateDatabaseUtil.createDatabase(
            "OSQLFunctionShortestPath", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    graph =
        orientDB.open("OSQLFunctionShortestPath", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    graph.createEdgeClass("Edge1");
    graph.createEdgeClass("Edge2");

    vertices.put(1, graph.newVertex().save());
    vertices.put(2, graph.newVertex().save());
    vertices.put(3, graph.newVertex().save());
    vertices.put(4, graph.newVertex().save());

    vertices.get(1).setProperty("node_id", "A");
    vertices.get(2).setProperty("node_id", "B");
    vertices.get(3).setProperty("node_id", "C");
    vertices.get(4).setProperty("node_id", "D");

    graph.newEdge(vertices.get(1), vertices.get(2), "Edge1").save();
    graph.newEdge(vertices.get(2), vertices.get(3), "Edge1").save();
    graph.newEdge(vertices.get(3), vertices.get(1), "Edge2").save();
    graph.newEdge(vertices.get(3), vertices.get(4), "Edge1").save();

    for (int i = 5; i <= 20; i++) {
      vertices.put(i, graph.newVertex().save());
      vertices.get(i).setProperty("node_id", "V" + i);
      graph.newEdge(vertices.get(i - 1), vertices.get(i), "Edge1").save();
      if (i % 2 == 0) {
        graph.newEdge(vertices.get(i - 2), vertices.get(i), "Edge1").save();
      }
    }
  }

  @Test
  public void testExecute() throws Exception {
    final List<ORID> result =
        function.execute(
            null,
            null,
            null,
            new Object[] {vertices.get(1), vertices.get(4)},
            new OBasicCommandContext());

    Assert.assertEquals(3, result.size());
    Assert.assertEquals(vertices.get(1).getIdentity(), result.get(0));
    Assert.assertEquals(vertices.get(3).getIdentity(), result.get(1));
    Assert.assertEquals(vertices.get(4).getIdentity(), result.get(2));
  }

  @Test
  public void testExecuteOut() throws Exception {
    final List<ORID> result =
        function.execute(
            null,
            null,
            null,
            new Object[] {vertices.get(1), vertices.get(4), "out", null},
            new OBasicCommandContext());

    Assert.assertEquals(4, result.size());
    Assert.assertEquals(vertices.get(1).getIdentity(), result.get(0));
    Assert.assertEquals(vertices.get(2).getIdentity(), result.get(1));
    Assert.assertEquals(vertices.get(3).getIdentity(), result.get(2));
    Assert.assertEquals(vertices.get(4).getIdentity(), result.get(3));
  }

  @Test
  public void testExecuteOnlyEdge1() throws Exception {
    final List<ORID> result =
        function.execute(
            null,
            null,
            null,
            new Object[] {vertices.get(1), vertices.get(4), null, "Edge1"},
            new OBasicCommandContext());

    Assert.assertEquals(4, result.size());
    Assert.assertEquals(vertices.get(1).getIdentity(), result.get(0));
    Assert.assertEquals(vertices.get(2).getIdentity(), result.get(1));
    Assert.assertEquals(vertices.get(3).getIdentity(), result.get(2));
    Assert.assertEquals(vertices.get(4).getIdentity(), result.get(3));
  }

  @Test
  public void testExecuteOnlyEdge1AndEdge2() throws Exception {
    final List<ORID> result =
        function.execute(
            null,
            null,
            null,
            new Object[] {vertices.get(1), vertices.get(4), "BOTH", asList("Edge1", "Edge2")},
            new OBasicCommandContext());

    Assert.assertEquals(3, result.size());
    Assert.assertEquals(vertices.get(1).getIdentity(), result.get(0));
    Assert.assertEquals(vertices.get(3).getIdentity(), result.get(1));
    Assert.assertEquals(vertices.get(4).getIdentity(), result.get(2));
  }

  @Test
  public void testLong() throws Exception {
    final List<ORID> result =
        function.execute(
            null,
            null,
            null,
            new Object[] {vertices.get(1), vertices.get(20)},
            new OBasicCommandContext());

    Assert.assertEquals(11, result.size());
    Assert.assertEquals(vertices.get(1).getIdentity(), result.get(0));
    Assert.assertEquals(vertices.get(3).getIdentity(), result.get(1));
    int next = 2;
    for (int i = 4; i <= 20; i += 2) {
      Assert.assertEquals(vertices.get(i).getIdentity(), result.get(next++));
    }
  }

  @Test
  public void testMaxDepth1() throws Exception {
    Map<String, Object> additionalParams = new HashMap<String, Object>();
    additionalParams.put(OSQLFunctionShortestPath.PARAM_MAX_DEPTH, 11);
    final List<ORID> result =
        function.execute(
            null,
            null,
            null,
            new Object[] {vertices.get(1), vertices.get(20), null, null, additionalParams},
            new OBasicCommandContext());

    Assert.assertEquals(11, result.size());
  }

  @Test
  public void testMaxDepth2() throws Exception {
    Map<String, Object> additionalParams = new HashMap<String, Object>();
    additionalParams.put(OSQLFunctionShortestPath.PARAM_MAX_DEPTH, 12);
    final List<ORID> result =
        function.execute(
            null,
            null,
            null,
            new Object[] {vertices.get(1), vertices.get(20), null, null, additionalParams},
            new OBasicCommandContext());

    Assert.assertEquals(11, result.size());
  }

  @Test
  public void testMaxDepth3() throws Exception {
    Map<String, Object> additionalParams = new HashMap<String, Object>();
    additionalParams.put(OSQLFunctionShortestPath.PARAM_MAX_DEPTH, 10);
    final List<ORID> result =
        function.execute(
            null,
            null,
            null,
            new Object[] {vertices.get(1), vertices.get(20), null, null, additionalParams},
            new OBasicCommandContext());

    Assert.assertEquals(0, result.size());
  }

  @Test
  public void testMaxDepth4() throws Exception {
    Map<String, Object> additionalParams = new HashMap<String, Object>();
    additionalParams.put(OSQLFunctionShortestPath.PARAM_MAX_DEPTH, 3);
    final List<ORID> result =
        function.execute(
            null,
            null,
            null,
            new Object[] {vertices.get(1), vertices.get(20), null, null, additionalParams},
            new OBasicCommandContext());

    Assert.assertEquals(0, result.size());
  }
}

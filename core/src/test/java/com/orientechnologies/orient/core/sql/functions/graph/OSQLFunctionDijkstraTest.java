package com.orientechnologies.orient.core.sql.functions.graph;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class OSQLFunctionDijkstraTest {

  private OrientDB          orientDB;
  private ODatabaseDocument graph;

  private OVertex              v1;
  private OVertex              v2;
  private OVertex              v3;
  private OVertex              v4;
  private OSQLFunctionDijkstra functionDijkstra;

  @Before
  public void setUp() throws Exception {
    setUpDatabase();

    functionDijkstra = new OSQLFunctionDijkstra();
  }

  @After
  public void tearDown() throws Exception {
    graph.close();
    orientDB.close();
  }

  private void setUpDatabase() {
    orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.createIfNotExists("OSQLFunctionDijkstraTest", ODatabaseType.MEMORY);
    graph = orientDB.open("OSQLFunctionDijkstraTest", "admin", "admin");

    graph.createEdgeClass("weight");

    v1 = graph.newVertex();
    v2 = graph.newVertex();
    v3 = graph.newVertex();
    v4 = graph.newVertex();

    v1.setProperty("node_id", "A");
    v2.setProperty("node_id", "B");
    v3.setProperty("node_id", "C");
    v4.setProperty("node_id", "D");

    OEdge e1 = graph.newEdge(v1, v2, "weight");
    e1.setProperty("weight", 1.0f);
    e1.save();

    OEdge e2 = graph.newEdge(v2, v3, "weight");
    e2.setProperty("weight", 1.0f);
    e2.save();

    OEdge e3 = graph.newEdge(v1, v3, "weight");
    e3.setProperty("weight", 100.0f);
    e3.save();

    OEdge e4 = graph.newEdge(v3, v4, "weight");
    e4.setProperty("weight", 1.0f);
    e4.save();
  }

  @Test
  public void testExecute() throws Exception {
    final List<OVertex> result = functionDijkstra
        .execute(null, null, null, new Object[] { v1, v4, "'weight'" }, new OBasicCommandContext());

    assertEquals(4, result.size());
    assertEquals(v1, result.get(0));
    assertEquals(v2, result.get(1));
    assertEquals(v3, result.get(2));
    assertEquals(v4, result.get(3));
  }
}

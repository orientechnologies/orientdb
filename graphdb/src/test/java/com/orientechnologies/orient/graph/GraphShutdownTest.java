package com.orientechnologies.orient.graph;

import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class GraphShutdownTest {

  @Test(expected = ODatabaseException.class)
  public void graphCommitAfterShutdown() {
    OrientGraphFactory factory = new OrientGraphFactory("memory:graphCommitAfterShutdown");
    OrientGraph graph1 = factory.getTx();
    OrientGraph graph2 = factory.getTx();
    graph2.shutdown(true); // in 2.2 this will not close the database because graph1 is still active in the pool
    graph2.commit(); // this should fail
    factory.drop();
  }

  @Test
  public void rollbackOnShutdownNotCommit() {
    OrientGraph orientGraph = new OrientGraph("memory:test");
    Map userData = new HashMap();

    orientGraph.addVertex("User", userData);

    orientGraph.shutdown(true, false);
    orientGraph = new OrientGraph("memory:test");
    assertEquals(orientGraph.countVertices(), 0);
  }

}
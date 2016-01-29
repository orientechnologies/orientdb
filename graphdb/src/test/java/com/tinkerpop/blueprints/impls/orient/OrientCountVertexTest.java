package com.tinkerpop.blueprints.impls.orient;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class OrientCountVertexTest {

  @After
  public void tearDown() {
    OrientGraph g = createGraph();
    g.drop();
  }

  @Test
  public void countVertexShouldWorkWithinOrOutsideTransactions() {

    // Create a node v1 with at least two edges
    OrientGraph g = createGraph();

    assertEquals(0, g.countVertices());
    g.addVertex("class:V1");
    g.addVertex("class:V2");
    g.addVertex("class:V2");

    g.shutdown();

    g = createGraph();

    long allCount = g.countVertices();
    assertEquals(3, allCount);
    long v1Count = g.countVertices("V1");
    assertEquals(1, v1Count);
    assertEquals(2, g.countVertices("V2"));


    g.addVertex("class:V1");
    assertEquals(allCount + 1, g.countVertices());
    assertEquals(v1Count + 1, g.countVertices("V1"));

    g.shutdown();
  }


  private OrientGraph createGraph() {
    return (OrientGraph) new OrientGraph("memory:" + OrientCountVertexTest.class.getSimpleName()).setWarnOnForceClosingTx(false);
  }

}

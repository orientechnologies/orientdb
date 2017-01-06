package com.orientechnologies.orient.graph;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNull;

/**
 * Created by tglman on 06/01/17.
 */
public class TestNotTxVertexDropWithRidBagTree {

  private OrientGraphNoTx graph;

  @Before
  public void before() {
    graph = new OrientGraphNoTx("memory:" + TestNotTxVertexDropWithRidBagTree.class.getSimpleName());
    graph.createVertexType("Test");
    graph.createEdgeType("Ref");
  }

  @Test
  public void testDropVertex() {
    OrientVertex vertex = graph.addVertex("class:Test");
    Object id = null;
    for (int i = 0; i < OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger() * 2; i++) {
      OrientVertex v1 = graph.addVertex("class:Test");
      id = vertex.addEdge("Ref", v1).getId();
    }

    vertex.remove();
    assertNull(graph.getEdge(id));
  }

  @After
  public void after() {
    graph.drop();
  }

}

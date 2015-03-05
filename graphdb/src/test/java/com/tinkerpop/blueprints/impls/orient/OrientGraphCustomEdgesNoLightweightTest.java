package com.tinkerpop.blueprints.impls.orient;

import com.tinkerpop.blueprints.Graph;
import org.hamcrest.core.AnyOf;
import org.hamcrest.core.IsEqual;
import org.junit.Assume;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test suite for OrientDB graph implementation that store edges using custom classes derived by labels.
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
@RunWith(JUnit4.class)
public class OrientGraphCustomEdgesNoLightweightTest extends OrientGraphTest {
  @Before
  public void setUp() throws Exception {
    Assume.assumeThat(getEnvironment(), AnyOf.anyOf(IsEqual.equalTo(ENV.CI), IsEqual.equalTo(ENV.RELEASE)));
    super.setUp();
  }

  public Graph generateGraph(final String graphDirectoryName) {
    final OrientGraph graph = (OrientGraph) super.generateGraph(graphDirectoryName);
    graph.setUseClassForEdgeLabel(true);
    graph.setUseLightweightEdges(false);

    if (graph.getEdgeType("friend") == null) {
        graph.createEdgeType("friend");
    }
    if (graph.getEdgeType("test") == null) {
        graph.createEdgeType("test");
    }
    if (graph.getEdgeType("knows") == null) {
        graph.createEdgeType("knows");
    }
    if (graph.getEdgeType("created") == null) {
        graph.createEdgeType("created");
    }
    if (graph.getEdgeType("collaborator") == null) {
        graph.createEdgeType("collaborator");
    }
    if (graph.getEdgeType("hate") == null) {
        graph.createEdgeType("hate");
    }
    if (graph.getEdgeType("hates") == null) {
        graph.createEdgeType("hates");
    }
    if (graph.getEdgeType("test-edge") == null) {
        graph.createEdgeType("test-edge");
    }
    if (graph.getEdgeType("self") == null) {
        graph.createEdgeType("self");
    }
    if (graph.getEdgeType("x") == null) {
        graph.createEdgeType("x");
    }
    if (graph.getEdgeType("y") == null) {
        graph.createEdgeType("y");
    }
    if (graph.getEdgeType("test1") == null) {
        graph.createEdgeType("test1");
    }
    if (graph.getEdgeType("test2") == null) {
        graph.createEdgeType("test2");
    }
    if (graph.getEdgeType("test3") == null) {
        graph.createEdgeType("test3");
    }

    return graph;
  }

}

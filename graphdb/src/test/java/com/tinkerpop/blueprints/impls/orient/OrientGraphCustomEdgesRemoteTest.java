package com.tinkerpop.blueprints.impls.orient;

import com.tinkerpop.blueprints.Graph;
import org.hamcrest.core.IsEqual;
import org.junit.Assume;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 2/6/14
 */
@RunWith(JUnit4.class)
public class OrientGraphCustomEdgesRemoteTest extends OrientGraphRemoteTest {
  @Before
  public void setUp() throws Exception {
    Assume.assumeThat(getEnvironment(), IsEqual.equalTo(ENV.RELEASE));
    super.setUp();
  }

  public Graph generateGraph(final String graphDirectoryName) {
    final OrientGraph graph = (OrientGraph) super.generateGraph(graphDirectoryName);

    if (graph.getEdgeType("friend") == null) graph.createEdgeType("friend");
    if (graph.getEdgeType("test") == null) graph.createEdgeType("test");
    if (graph.getEdgeType("knows") == null) graph.createEdgeType("knows");
    if (graph.getEdgeType("created") == null) graph.createEdgeType("created");
    if (graph.getEdgeType("collaborator") == null) graph.createEdgeType("collaborator");
    if (graph.getEdgeType("hate") == null) graph.createEdgeType("hate");
    if (graph.getEdgeType("hates") == null) graph.createEdgeType("hates");
    if (graph.getEdgeType("test-edge") == null) graph.createEdgeType("test-edge");
    if (graph.getEdgeType("self") == null) graph.createEdgeType("self");
    if (graph.getEdgeType("pets") == null) graph.createEdgeType("pets");

    return graph;
  }
}

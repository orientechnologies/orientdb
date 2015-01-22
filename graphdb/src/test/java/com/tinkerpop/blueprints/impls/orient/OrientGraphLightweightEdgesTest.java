package com.tinkerpop.blueprints.impls.orient;

import com.tinkerpop.blueprints.Graph;
import org.hamcrest.core.AnyOf;
import org.hamcrest.core.IsEqual;
import org.junit.Assume;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test suite for OrientDB graph implementation that uses Lightweight edges.
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
@RunWith(JUnit4.class)
public class OrientGraphLightweightEdgesTest extends OrientGraphTest {
  @Before
  public void setUp() throws Exception {
    Assume.assumeThat(getEnvironment(), AnyOf.anyOf(IsEqual.equalTo(ENV.CI), IsEqual.equalTo(ENV.RELEASE)));
    super.setUp();
  }

  public Graph generateGraph(final String graphDirectoryName) {
    OrientGraph graph = (OrientGraph) super.generateGraph(graphDirectoryName);
    graph.setUseLightweightEdges(true);
    graph.setUseClassForEdgeLabel(false);
    graph.setUseClassForVertexLabel(false);
    return graph;
  }

}

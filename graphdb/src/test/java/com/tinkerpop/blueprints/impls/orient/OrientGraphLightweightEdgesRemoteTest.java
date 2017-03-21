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
public class OrientGraphLightweightEdgesRemoteTest extends OrientGraphRemoteTest {
  @Before
  public void setUp() throws Exception {
    Assume.assumeThat(getEnvironment(), IsEqual.equalTo(ENV.RELEASE));
    super.setUp();
  }

  public Graph generateGraph(final String graphDirectoryName) {
    OrientGraph graph = (OrientGraph) super.generateGraph(graphDirectoryName);
    graph.setUseLightweightEdges(true);
    graph.setUseClassForVertexLabel(false);
    return graph;
  }
}

package com.tinkerpop.blueprints.impls.orient;

import com.tinkerpop.blueprints.Graph;
import org.hamcrest.core.AnyOf;
import org.hamcrest.core.IsEqual;
import org.junit.Assume;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.JUnit4;
import org.junit.runners.Suite;

import java.util.Arrays;

/**
 * Test suite for OrientDB graph implementation compatible with the classic one before OrientDB 1.4.
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
@RunWith(JUnit4.class)
public class OrientGraphClassicTest extends OrientGraphTest {
  @Before
  public void setUp() throws Exception {
    Assume.assumeThat(getEnvironment(), AnyOf.anyOf(IsEqual.equalTo(ENV.CI), IsEqual.equalTo(ENV.RELEASE)));
    super.setUp();
  }

  public Graph generateGraph(final String graphDirectoryName) {
    this.currentGraph = (OrientGraph) super.generateGraph(graphDirectoryName);
    this.currentGraph.setUseLightweightEdges(false);
    this.currentGraph.setUseClassForEdgeLabel(false);
    this.currentGraph.setUseClassForVertexLabel(false);
    this.currentGraph.setUseVertexFieldsForEdgeLabels(false);
    return this.currentGraph;
  }
}

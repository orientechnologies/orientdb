package com.tinkerpop.blueprints.impls.orient;

import com.tinkerpop.blueprints.Graph;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test suite for OrientDB graph implementation compatible with the classic one before OrientDB 1.4.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (http://orientdb.com)
 */
@RunWith(JUnit4.class)
public class OrientGraphClassicTestIT extends OrientGraphTest {

  public Graph generateGraph(final String graphDirectoryName) {
    OrientGraph graph = (OrientGraph) super.generateGraph(graphDirectoryName);
    graph.setUseLightweightEdges(false);
    graph.setUseVertexFieldsForEdgeLabels(false);
    return graph;
  }
}

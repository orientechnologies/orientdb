package com.tinkerpop.blueprints.impls.orient;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.tinkerpop.blueprints.Graph;

/**
 * Test suite to test avoiding of using TX in SQL commands.
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
@RunWith(JUnit4.class)
public class OrientGraphNoTxSQLTest extends OrientGraphNoTxTest {
  @Before
  public void setUp() throws Exception {
    // Assume.assumeThat(getEnvironment(), AnyOf.anyOf(IsEqual.equalTo(ENV.CI), IsEqual.equalTo(ENV.RELEASE)));
    super.setUp();
  }

  public Graph generateGraph(final String graphDirectoryName) {
    OrientBaseGraph graph = (OrientBaseGraph) super.generateGraph(graphDirectoryName);
    graph.setTxRequiredForSQLGraphOperations(false);
    return graph;
  }
}

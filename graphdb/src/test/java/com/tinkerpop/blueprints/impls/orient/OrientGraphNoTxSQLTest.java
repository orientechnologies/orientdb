package com.tinkerpop.blueprints.impls.orient;

import com.tinkerpop.blueprints.Graph;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test suite to test avoiding of using TX in SQL commands.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (http://orientdb.com)
 */
@RunWith(JUnit4.class)
public class OrientGraphNoTxSQLTest extends OrientGraphNoTxTest {
  @Before
  public void setUp() throws Exception {
    // Assume.assumeThat(getEnvironment(), AnyOf.anyOf(IsEqual.equalTo(ENV.CI),
    // IsEqual.equalTo(ENV.RELEASE)));
    super.setUp();
  }

  public Graph generateGraph(final String graphDirectoryName) {
    OrientBaseGraph graph = (OrientBaseGraph) super.generateGraph(graphDirectoryName);
    graph.setTxRequiredForSQLGraphOperations(false);
    return graph;
  }
}

package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQueryTestSuite;
import com.tinkerpop.blueprints.IndexableGraphTestSuite;
import com.tinkerpop.blueprints.KeyIndexableGraphTestSuite;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.VertexQueryTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.gml.GMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReaderTestSuite;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(JUnit4.class)
public class OrientGraphNoTxTest extends GraphTest {
  private Map<String, OrientGraphNoTx> currentGraphs = new HashMap<String, OrientGraphNoTx>();

  // testing only those suites that are read-only

  @Test
  public void testVertexQueryTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new VertexQueryTestSuite(this));
    printTestPerformance("VertexQueryTestSuite", this.stopWatch());
  }

  @Test
  public void testGraphQueryTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new GraphQueryTestSuite(this));
    printTestPerformance("GraphQueryTestSuite", this.stopWatch());
  }

  @Test
  public void testIndexableGraphTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new IndexableGraphTestSuite(this));
    printTestPerformance("IndexableGraphTestSuite", this.stopWatch());
  }

  @Test
  public void testKeyIndexableGraphTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new KeyIndexableGraphTestSuite(this));
    printTestPerformance("KeyIndexableGraphTestSuite", this.stopWatch());
  }

  @Test
  public void testGraphMLReaderTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new GraphMLReaderTestSuite(this));
    printTestPerformance("GraphMLReaderTestSuite", this.stopWatch());
  }

  @Test
  public void testGraphSONReaderTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new GraphSONReaderTestSuite(this));
    printTestPerformance("GraphSONReaderTestSuite", this.stopWatch());
  }

  @Test
  public void testGMLReaderTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new GMLReaderTestSuite(this));
    printTestPerformance("GMLReaderTestSuite", this.stopWatch());
  }

  public Graph generateGraph() {
    return generateGraph("graph");
  }

  public Graph generateGraph(final String graphDirectoryName) {
    final String url = OrientGraphTest.getStorageType() + ":" + getWorkingDirectory() + "/" + graphDirectoryName;

    OrientGraphNoTx graph = currentGraphs.get(url);

    if (graph != null) {
      if (graph.isClosed())
        currentGraphs.remove(url);
      else {
        ODatabaseRecordThreadLocal.INSTANCE.set(graph.getRawGraph());
        return graph;
      }
    }

    graph = new OrientGraphNoTx(url);
    graph.setWarnOnForceClosingTx(false);
    graph.setStandardExceptions(true);

    currentGraphs.put(url, graph);

    return graph;
  }

  public void doTestSuite(final TestSuite testSuite) throws Exception {
    String directory = getWorkingDirectory();
    deleteDirectory(new File(directory));
    for (Method method : testSuite.getClass().getDeclaredMethods()) {
      if (method.getName().startsWith("test")) {
        System.out.println("Testing " + method.getName() + "...");
        method.invoke(testSuite);
        dropGraph("graph");
      }
    }
  }

  private String getWorkingDirectory() {
    return this.computeTestDataRoot().getAbsolutePath();
  }

  @Override
  public void dropGraph(String graphDirectoryName) {
    final String graphDirectory = getWorkingDirectory() + "/" + graphDirectoryName;
    final String url = OrientGraphTest.getStorageType() +  ":" + graphDirectory;
    try {
      OrientGraphNoTx graph = currentGraphs.remove(url);
      if (graph == null || graph.isClosed())
        graph = new OrientGraphNoTx(url);

      graph.drop();
    } catch (Exception e) {
      e.printStackTrace();
    }

    deleteDirectory(new File(graphDirectory));
  }
}

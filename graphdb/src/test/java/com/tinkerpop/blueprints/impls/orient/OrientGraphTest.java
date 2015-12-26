package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.gml.GMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReaderTestSuite;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Test suite for OrientDB graph implementation.
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public abstract class OrientGraphTest extends GraphTest {

  protected Map<String, OrientGraph> currentGraphs = new HashMap<String, OrientGraph>();

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testVertexTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new VertexTestSuite(this));
    printTestPerformance("VertexTestSuite", this.stopWatch());
  }

  @Test
  public void testEdgeTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new EdgeTestSuite(this));
    printTestPerformance("EdgeTestSuite", this.stopWatch());
  }

  @Test
  public void testGraphTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new GraphTestSuite(this));
    printTestPerformance("GraphTestSuite", this.stopWatch());
  }

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
  public void testIndexTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new IndexTestSuite(this));
    printTestPerformance("IndexTestSuite", this.stopWatch());
  }

  @Test
  public void testKeyIndexableGraphTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new KeyIndexableGraphTestSuite(this));
    printTestPerformance("KeyIndexableGraphTestSuite", this.stopWatch());
  }

  @Test
  public void testTransactionalGraphTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new TransactionalGraphTestSuite(this));
    printTestPerformance("TransactionGraphTestSuite", this.stopWatch());
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

  //
  // orientdb specific test
  //
  @Test
  public void testOrientGraphSpecificTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new OrientGraphSpecificTestSuite(this));
    printTestPerformance("OrientGraphSpecificTestSuite", this.stopWatch());
  }

  public Graph generateGraph() {
    return generateGraph("graph");
  }

  public Graph generateGraph(final String graphDirectoryName) {
    final String url = getStorageType() + ":" + getWorkingDirectory() + "/" + graphDirectoryName;

    OrientGraph graph = currentGraphs.get(url);

    if (graph != null) {
      if (graph.isClosed())
        currentGraphs.remove(url);
      else {
        ODatabaseRecordThreadLocal.INSTANCE.set(graph.getRawGraph());
        return graph;
      }
    }

    graph = new OrientGraph(url);
    graph.setWarnOnForceClosingTx(false);
    graph.setStandardExceptions(true);

    currentGraphs.put(url, graph);

    return graph;
  }

  public void doTestSuite(final TestSuite testSuite) throws Exception {
    dropGraph("graph");
    for (Method method : testSuite.getClass().getDeclaredMethods()) {
      if (method.getName().startsWith("test")) {
        System.out.println("Testing " + method.getName() + "...");
        method.invoke(testSuite);
        dropGraph("graph");
      }
    }
  }

  @Override
  public void dropGraph(final String graphDirectoryName) {
    // this is necessary on windows systems: deleting the directory is not enough because it takes a
    // while to unlock files

    final String graphDirectory = getWorkingDirectory() + "/" + graphDirectoryName;
    final String url = getStorageType() + ":" + graphDirectory;
    try {
      OrientGraph graph = currentGraphs.remove(url);
      if (graph == null || graph.isClosed())
        graph = new OrientGraph(url);

      graph.drop();
    } catch (Exception e) {
      e.printStackTrace();
    }

    deleteDirectory(new File(graphDirectory));
  }

  protected String getWorkingDirectory() {
    return this.computeTestDataRoot().getAbsolutePath();
  }

  public static enum ENV {
    DEV, RELEASE, CI
  }

  public static ENV getEnvironment() {
    String envName = System.getProperty("orientdb.test.env", "dev").toUpperCase();
    ENV result = null;
    try {
      result = ENV.valueOf(envName);
    } catch (IllegalArgumentException e) {
    }

    if (result == null)
      result = ENV.DEV;

    return result;
  }

  public static String getStorageType() {
    if (getEnvironment().equals(ENV.DEV))
      return "memory";

    return "plocal";
  }
}

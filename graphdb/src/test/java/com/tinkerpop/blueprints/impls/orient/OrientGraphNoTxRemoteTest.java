package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQueryTestSuite;
import com.tinkerpop.blueprints.KeyIndexableGraphTestSuite;
import com.tinkerpop.blueprints.TestSuite;
import com.tinkerpop.blueprints.VertexQueryTestSuite;
import com.tinkerpop.blueprints.impls.GraphTest;
import com.tinkerpop.blueprints.util.io.gml.GMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReaderTestSuite;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReaderTestSuite;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.hamcrest.core.IsEqual;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 2/6/14
 */
@RunWith(JUnit4.class)
public class OrientGraphNoTxRemoteTest extends GraphTest {
  private static final String serverPort = System.getProperty("orient.server.port", "3080");
  private static OServer server;
  private static String oldOrientDBHome;

  private static String serverHome;

  private Map<String, OrientGraphNoTx> currentGraphs = new HashMap<String, OrientGraphNoTx>();

  private Map<String, OrientGraphFactory> graphFactories =
      new HashMap<String, OrientGraphFactory>();

  @BeforeClass
  public static void startEmbeddedServer() throws Exception {
    final String buildDirectory = System.getProperty("buildDirectory", ".");
    serverHome = buildDirectory + "/" + OrientGraphNoTxRemoteTest.class.getSimpleName();

    File file = new File(serverHome);
    deleteDirectory(file);

    file = new File(serverHome);
    Assert.assertTrue(file.mkdir());

    oldOrientDBHome = System.getProperty("ORIENTDB_HOME");
    System.setProperty("ORIENTDB_HOME", serverHome);

    server = new OServer(false);
    server.startup(OrientGraphRemoteTest.class.getResourceAsStream("/embedded-server-config.xml"));
    server.activate();
  }

  @AfterClass
  public static void stopEmbeddedServer() throws Exception {
    server.shutdown();
    Thread.sleep(1000);
    ODatabaseDocumentTx.closeAll();

    Orient.instance().shutdown();
    Orient.instance().startup();

    if (oldOrientDBHome != null) System.setProperty("ORIENTDB_HOME", oldOrientDBHome);
    else System.clearProperty("ORIENTDB_HOME");

    final File file = new File(serverHome);
    deleteDirectory(file);
    OGlobalConfiguration.NETWORK_LOCK_TIMEOUT.setValue(15000);
  }

  @Before
  public void setUp() throws Exception {
    Assume.assumeThat(
        System.getProperty("orientdb.test.env", "dev").toUpperCase(Locale.ENGLISH),
        IsEqual.equalTo("RELEASE"));
    super.setUp();
  }

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
    final String url = "remote:localhost:" + serverPort + "/" + graphDirectoryName;
    OrientGraphNoTx graph = currentGraphs.get(url);

    if (graph != null) {
      if (graph.isClosed()) currentGraphs.remove(url);
      else {
        ODatabaseRecordThreadLocal.instance().set(graph.getRawGraph());
        return graph;
      }
    }

    try {
      final OServerAdmin serverAdmin = new OServerAdmin(url);
      serverAdmin.connect("root", "root");
      if (!serverAdmin.existsDatabase(OrientGraphTest.getStorageType()))
        serverAdmin.createDatabase("graph", OrientGraphTest.getStorageType());

      serverAdmin.close();

    } catch (IOException e) {
      throw new IllegalStateException(e);
    }

    OrientGraphFactory factory = graphFactories.get(url);
    if (factory == null) {
      factory = new OrientGraphFactory(url);
      factory.setupPool(5, 256);
      graphFactories.put(url, factory);
    }

    graph = factory.getNoTx();
    graph.setWarnOnForceClosingTx(false);
    graph.setStandardExceptions(true);

    currentGraphs.put(url, graph);

    return graph;
  }

  @Override
  public void dropGraph(final String graphDirectoryName) {
    // this is necessary on windows systems: deleting the directory is not enough because it takes a
    // while to unlock files
    try {
      final String url = "remote:localhost:" + serverPort + "/" + graphDirectoryName;
      final OrientGraphNoTx graph = currentGraphs.get(url);
      if (graph != null) graph.shutdown();

      final OrientGraphFactory factory = graphFactories.remove(url);
      if (factory != null) factory.close();

      final OServerAdmin serverAdmin = new OServerAdmin(url);
      serverAdmin.connect("root", "root");

      if (serverAdmin.existsDatabase(OrientGraphTest.getStorageType()))
        serverAdmin.dropDatabase(OrientGraphTest.getStorageType());

      serverAdmin.close();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public void doTestSuite(final TestSuite testSuite) throws Exception {
    for (Method method : testSuite.getClass().getDeclaredMethods()) {
      if (method.getName().startsWith("test")) {
        System.out.println("Testing " + method.getName() + "...");
        method.invoke(testSuite);
        dropGraph("graph");
      }
    }
  }
}

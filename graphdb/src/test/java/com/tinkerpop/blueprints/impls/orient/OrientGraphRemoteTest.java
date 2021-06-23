package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.IndexTestSuite;
import com.tinkerpop.blueprints.IndexableGraphTestSuite;
import com.tinkerpop.blueprints.KeyIndexableGraphTestSuite;
import com.tinkerpop.blueprints.TestSuite;
import java.io.File;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 2/6/14
 */
public abstract class OrientGraphRemoteTest extends OrientGraphTest {
  private static final String serverPort = System.getProperty("orient.server.port", "3080");
  private static OServer server;
  private static String oldOrientDBHome;

  private static String serverHome;

  private static OrientDB clientContext;

  private final Map<String, OrientGraphFactory> graphFactories = new HashMap<>();

  @BeforeClass
  public static void startEmbeddedServer() throws Exception {
    final String buildDirectory = System.getProperty("buildDirectory", ".");
    serverHome = buildDirectory + "/" + OrientGraphRemoteTest.class.getSimpleName();

    File file = new File(serverHome);
    deleteDirectory(file);

    file = new File(serverHome);
    Assert.assertTrue(file.mkdir());

    oldOrientDBHome = System.getProperty("ORIENTDB_HOME");
    System.setProperty("ORIENTDB_HOME", serverHome);

    server = new OServer(false);
    server.startup(OrientGraphRemoteTest.class.getResourceAsStream("/embedded-server-config.xml"));
    server.activate();

    clientContext =
        new OrientDB(
            "remote:localhost:" + serverPort, "root", "root", OrientDBConfig.defaultConfig());
  }

  @AfterClass
  public static void stopEmbeddedServer() throws Exception {
    server.shutdown();
    Thread.sleep(1000);
    clientContext.close();

    if (oldOrientDBHome != null) System.setProperty("ORIENTDB_HOME", oldOrientDBHome);
    else System.clearProperty("ORIENTDB_HOME");

    final File file = new File(serverHome);
    deleteDirectory(file);
  }

  public Graph generateGraph(final String graphDirectoryName) {
    final String url = "remote:localhost:" + serverPort + "/" + graphDirectoryName;
    OrientGraph graph = currentGraphs.get(url);

    if (graph != null) {
      if (graph.isClosed()) currentGraphs.remove(url);
      else return graph;
    }

    if (!clientContext.exists(graphDirectoryName)) {
      clientContext.execute(
          "create database "
              + graphDirectoryName
              + " "
              + ODatabaseType.valueOf(OrientGraphTest.getStorageType().toUpperCase())
              + " users ( admin identified by 'admin' role admin)");
    }

    OrientGraphFactory factory = graphFactories.get(url);
    if (factory == null) {
      factory = new OrientGraphFactory(url);

      factory.setupPool(5, 256);
      graphFactories.put(url, factory);
    }

    graph = factory.getTx();
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
      final OrientGraph graph = currentGraphs.get(url);
      if (graph != null && !graph.isClosed()) graph.shutdown();

      final OrientGraphFactory factory = graphFactories.remove(url);
      if (factory != null) factory.close();

      if (clientContext.exists(graphDirectoryName)) {
        clientContext.drop(graphDirectoryName);
      }
      ODatabaseDocumentTx.closeAll();

    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void doTestSuite(final TestSuite testSuite) throws Exception {
    for (Method method : testSuite.getClass().getDeclaredMethods()) {
      if (method.getName().startsWith("test")) {
        System.out.println("Testing " + method.getName() + "...");
        method.invoke(testSuite);
        dropGraph("graph");
      }
    }
  }

  @Test
  @Ignore
  @Override
  public void testIndexableGraphTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new IndexableGraphTestSuite(this));
    printTestPerformance("IndexableGraphTestSuite", this.stopWatch());
  }

  @Test
  @Ignore
  @Override
  public void testIndexTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new IndexTestSuite(this));
    printTestPerformance("IndexTestSuite", this.stopWatch());
  }

  @Test
  @Ignore
  @Override
  public void testKeyIndexableGraphTestSuite() throws Exception {
    this.stopWatch();
    doTestSuite(new KeyIndexableGraphTestSuite(this));
    printTestPerformance("KeyIndexableGraphTestSuite", this.stopWatch());
  }

  @Test
  public void testDeleteAndAddNewEdge() {
    OrientGraph graph = (OrientGraph) generateGraph();
    try {
      OrientVertex v1 = graph.addTemporaryVertex("Test1V");
      v1.getRecord().field("name", "v1");
      v1.save();

      OrientVertex v2 = graph.addTemporaryVertex("Test2V");
      v2.getRecord().field("name", "v2");
      v2.save();

      graph.commit();

      Assert.assertTrue(v1.getIdentity().isPersistent());
      Assert.assertTrue(v2.getIdentity().isPersistent());

      for (int i = 0; i < 5; i++) {
        System.out.println(i);

        // Remove all edges
        for (Edge edge : v1.getEdges(Direction.OUT, "TestE")) {
          edge.remove();
        }
        // Add new edge
        v1.addEdge("TestE", v2);

        graph.commit();

        Assert.assertEquals(
            v2.getId(), v1.getVertices(Direction.OUT, "TestE").iterator().next().getId());
        Assert.assertEquals(
            v1.getId(), v2.getVertices(Direction.IN, "TestE").iterator().next().getId());
      }

      graph.shutdown();
    } finally {
      dropGraph("graph");
    }
  }
}

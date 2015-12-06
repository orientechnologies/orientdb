package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.tinkerpop.blueprints.*;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 2/6/14
 */
public abstract class OrientGraphRemoteTest extends OrientGraphTest {
  private static OServer                  server;
  private static String                   oldOrientDBHome;

  private static String                   serverHome;

  private Map<String, OrientGraphFactory> graphFactories = new HashMap<String, OrientGraphFactory>();

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

    server = OServerMain.create();
    server.startup(OrientGraphRemoteTest.class.getResourceAsStream("/embedded-server-config.xml"));
    server.activate();
  }

  @AfterClass
  public static void stopEmbeddedServer() throws Exception {
    server.shutdown();
    Thread.sleep(1000);

    if (oldOrientDBHome != null)
      System.setProperty("ORIENTDB_HOME", oldOrientDBHome);
    else
      System.clearProperty("ORIENTDB_HOME");

    final File file = new File(serverHome);
    deleteDirectory(file);

    Orient.instance().startup();
  }

  public Graph generateGraph(final String graphDirectoryName) {
    final String url = "remote:localhost:3080/" + graphDirectoryName;
    OrientGraph graph = currentGraphs.get(url);

    if (graph != null) {
      if (graph.isClosed())
        currentGraphs.remove(url);
      else
        return graph;
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
      final String url = "remote:localhost:3080/" + graphDirectoryName;
      final OrientGraph graph = currentGraphs.get(url);
      if (graph != null)
        graph.shutdown();

      final OrientGraphFactory factory = graphFactories.remove(url);
      if (factory != null)
        factory.close();

      final OServerAdmin serverAdmin = new OServerAdmin(url);
      serverAdmin.connect("root", "root");

      if (serverAdmin.existsDatabase(OrientGraphTest.getStorageType()))
        serverAdmin.dropDatabase(OrientGraphTest.getStorageType());

      serverAdmin.close();
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

}

package com.tinkerpop.blueprints.impls.orient;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.*;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.tinkerpop.blueprints.Vertex;

public class OrientGraphMultithreadRemoteTest {
  private static OServer     server;
  private static String      oldOrientDBHome;

  private static String      serverHome;

  private OrientGraphFactory graphFactory;

  @BeforeClass
  public static void startEmbeddedServer() throws Exception {
    final String buildDirectory = System.getProperty("buildDirectory", ".");
    serverHome = buildDirectory + "/" + OrientGraphMultithreadRemoteTest.class.getSimpleName();

    File file = new File(serverHome);
    deleteDirectory(file);

    file = new File(serverHome);
    Assert.assertTrue(file.mkdir());

    oldOrientDBHome = System.getProperty("ORIENTDB_HOME");
    System.setProperty("ORIENTDB_HOME", serverHome);

    server = OServerMain.create();
    server.startup(OrientGraphMultithreadRemoteTest.class.getResourceAsStream("/embedded-server-config.xml"));
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

  @Before
  public void before() {
    OGlobalConfiguration.NETWORK_LOCK_TIMEOUT.setValue(15000);
    final String url = "remote:localhost:3080/" + OrientGraphMultithreadRemoteTest.class.getSimpleName();

    try {
      final OServerAdmin serverAdmin = new OServerAdmin(url);
      serverAdmin.connect("root", "root");
      if (!serverAdmin.existsDatabase(OrientGraphTest.getStorageType()))
        serverAdmin.createDatabase("graph", OrientGraphTest.getStorageType());

      serverAdmin.close();

    } catch (IOException e) {
      throw new IllegalStateException(e);
    }

    graphFactory = new OrientGraphFactory(url);
    graphFactory.setupPool(5, 256);

  }

  @Test
  public void testThreadingInsert() throws InterruptedException {
    List<Thread> threads = new ArrayList<Thread>();
    int threadCount = 8;
    final int recordsPerThread = 20;
    long records = threadCount * recordsPerThread;
    try {
      for (int t = 0; t < threadCount; t++) {
        Thread thread = new Thread() {
          @Override
          public void run() {
            for (int i = 0; i < recordsPerThread; i++) {
              OrientGraph graph = graphFactory.getTx(); // get an instance from the pool
              try {

                Vertex v1 = graph.addVertex(null);
                v1.setProperty("name", "b");
                // v1.setProperty("blob", blob);

                graph.commit(); // commits transaction
              } catch (Exception ex) {
                try {
                  graph.rollback();
                } catch (Exception ex1) {
                  System.out.println("rollback exception! " + ex);
                }

                System.out.println("operation exception! " + ex);
                ex.printStackTrace(System.out);
              } finally {
                graph.shutdown();
              }
            }
          }
        };
        threads.add(thread);
        thread.start();
      }
    } catch (Exception ex) {
      System.err.println("instance exception! " + ex);
      System.out.println("instance exception! " + ex);
      ex.printStackTrace(System.err);
    } finally {
      for (Thread t : threads) {
        t.join();
      }
    }
    OrientGraph graph = graphFactory.getTx();

    long actualRecords = graph.countVertices();

    if (actualRecords != records) {
      System.out
          .println("Count of records on server does not equal to expected count of records. Try to reproduce it next 10 times");

      int reproduced = 0;
      while (true) {
        if (graph.countVertices() != records)
          reproduced++;
        else
          break;
        if (reproduced == 10) {
          System.out.println("Test goes in forever loop to investigate reason of this error.");
          while (true)
            ;
        }
      }

      Assert.fail();
    }

    graph.shutdown();

  }

  @After
  public void after() {
    graphFactory.close();
  }

  protected static void deleteDirectory(final File directory) {
    if (directory.exists()) {
      for (File file : directory.listFiles()) {
        if (file.isDirectory()) {
          deleteDirectory(file);
        } else {
          file.delete();
        }
      }
      directory.delete();
    }
  }

}

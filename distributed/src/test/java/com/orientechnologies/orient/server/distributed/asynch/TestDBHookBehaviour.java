package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.orient.core.Orient;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import junit.framework.TestCase;

import java.io.File;

public class TestDBHookBehaviour extends TestCase {

  private static final String CONFIG_DIR             = "src/test/resources";
  private static final String DB1_DIR                = "target/db1";
  private static final String SERVER_ORIENT_URL_MAIN = "plocal:" + DB1_DIR + "/databases/testDB";

  private volatile Throwable  exceptionInThread;

  public void testDBHookFire() throws Throwable {
    Orient.setRegisterDatabaseByPath(true);

    // Start the first DB server.
    Thread dbServer1 = new Thread() {
      @Override
      public void run() {
        dbServer(DB1_DIR, SERVER_ORIENT_URL_MAIN, "asynch-dserver-config-0.xml");
      }
    };

    dbServer1.start();
    dbServer1.join();

    // Start the first DB client.
    Thread dbClient1 = new Thread() {
      @Override
      public void run() {
        dbClient1();
      }
    };

    dbClient1.start();
    dbClient1.join();

    if (exceptionInThread != null) {
      throw exceptionInThread;
    }
  }

  private void dbServer(String dbDirectory, String orientUrl, String dbConfigName) {
    BareBonesServer dbServer = new BareBonesServer();
    dbServer.deleteRecursively(new File(dbDirectory));
    if (orientUrl != null) {
      dbServer.createDB(orientUrl);
    }
    System.setProperty("ORIENTDB_HOME", dbDirectory);
    dbServer.start(CONFIG_DIR, dbConfigName);
  }

  private void dbClient1() {
    OrientBaseGraph graph = new OrientGraph(SERVER_ORIENT_URL_MAIN);
    try {
      Vertex v1 = graph.addVertex("vertextype", (String) null);
      graph.commit();
      assertEquals(1, HookFireCounter.getVertexCreatedCnt());
      assertEquals(0, HookFireCounter.getVertexUpdatedCnt());
      assertEquals(0, HookFireCounter.getEdgeCreatedCnt());
      assertEquals(0, HookFireCounter.getEdgeUpdatedCnt());

      v1.setProperty("p1a", "v1a");
      graph.commit();
      assertEquals(1, HookFireCounter.getVertexCreatedCnt());
      assertEquals(1, HookFireCounter.getVertexUpdatedCnt());
      assertEquals(0, HookFireCounter.getEdgeCreatedCnt());
      assertEquals(0, HookFireCounter.getEdgeUpdatedCnt());

      Vertex v2 = graph.addVertex("vertextype", (String) null);
      graph.commit();
      assertEquals(2, HookFireCounter.getVertexCreatedCnt());
      assertEquals(1, HookFireCounter.getVertexUpdatedCnt());
      assertEquals(0, HookFireCounter.getEdgeCreatedCnt());
      assertEquals(0, HookFireCounter.getEdgeUpdatedCnt());

      v2.setProperty("p2a", "v2a");
      graph.commit();
      assertEquals(2, HookFireCounter.getVertexCreatedCnt());
      assertEquals(2, HookFireCounter.getVertexUpdatedCnt());
      assertEquals(0, HookFireCounter.getEdgeCreatedCnt());
      assertEquals(0, HookFireCounter.getEdgeUpdatedCnt());

      v1.addEdge("edgetype", v2);
      graph.commit();
      assertEquals(2, HookFireCounter.getVertexCreatedCnt());
      assertEquals(4, HookFireCounter.getVertexUpdatedCnt());
      assertEquals(1, HookFireCounter.getEdgeCreatedCnt());
      assertEquals(1, HookFireCounter.getEdgeUpdatedCnt());
    } catch (Throwable e) {
      if (exceptionInThread == null) {
        exceptionInThread = e;
      }
    } finally {
      graph.shutdown();
    }
  }

}

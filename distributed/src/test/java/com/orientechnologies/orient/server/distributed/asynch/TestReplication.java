package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import junit.framework.TestCase;

import java.io.File;

public class TestReplication extends TestCase {

  private static final String CONFIG_DIR                = "src/test/resources";
  private static final String DB1_DIR                   = "target/db1";
  private static final String DB2_DIR                   = "target/db2";
  private static final String SERVER_ORIENT_URL_MAIN    = "plocal:" + DB1_DIR + "/databases/testDB";
  /* private static final String SERVER_ORIENT_URL_REPLICA = "plocal:" + DB2_DIR + "/databases/testDB"; */
  private static final String CLIENT_ORIENT_URL_MAIN    = "remote:localhost:2424/testDB";
  private static final String CLIENT_ORIENT_URL_REPLICA = "remote:localhost:2425/testDB";

  private static Object       LOCK                      = new Object();
  private Object              vertex1Id, vertex2Id;
  private volatile Throwable  exceptionInThread;

  public void testReplication() throws Throwable {
    Orient.setRegisterDatabaseByPath(true);

    final BareBonesServer[] servers = new BareBonesServer[2];

    // Start the first DB server.
    Thread dbServer1 = new Thread() {
      @Override
      public void run() {
        servers[0] = dbServer(DB1_DIR, SERVER_ORIENT_URL_MAIN, "asynch-dserver-config-0.xml");
      }
    };
    dbServer1.start();
    dbServer1.join();

    // Start the second DB server.
    Thread dbServer2 = new Thread() {
      @Override
      public void run() {
        servers[1] = dbServer(DB2_DIR, null, "asynch-dserver-config-1.xml");
      }
    };
    dbServer2.start();
    dbServer2.join();

    // Start the first DB client.
    Thread dbClient1 = new Thread() {
      @Override
      public void run() {
        dbClient1();
      }
    };

    dbClient1.start();
    // Start the second DB client.
    Thread dbClient2 = new Thread() {
      @Override
      public void run() {
        dbClient2();
      }
    };
    dbClient2.start();

    dbClient1.join();
    dbClient2.join();

    if (exceptionInThread != null) {
      throw exceptionInThread;
    }

    servers[0].stop();
    servers[1].stop();
  }

  private BareBonesServer dbServer(String dbDirectory, String orientUrl, String dbConfigName) {
    BareBonesServer dbServer = new BareBonesServer();
    dbServer.deleteRecursively(new File(dbDirectory));
    if (orientUrl != null) {
      dbServer.createDB(orientUrl);
    }
    System.setProperty("ORIENTDB_HOME", dbDirectory);
    dbServer.start(CONFIG_DIR, dbConfigName);
    return dbServer;
  }

  private void dbClient1() {
    sleep(1000);

    synchronized (LOCK) {
      OrientBaseGraph graph = new OrientGraph(CLIENT_ORIENT_URL_MAIN/* SERVER_ORIENT_URL_MAIN */);
      try {
        Vertex v1 = graph.addVertex("VertexType1", (String) null);
        graph.commit();
        vertex1Id = v1.getId();
        Vertex v2 = graph.addVertex("VertexType1", (String) null);
        graph.commit();
        vertex2Id = v2.getId();

        v1.setProperty("p1", "v1");
        graph.commit();

        v2.setProperty("p2", "v2");
        graph.commit();

        pause();
      } catch (Throwable e) {
        if (exceptionInThread == null) {
          exceptionInThread = e;
        }
      } finally {
        OLogManager.instance().info(this, "Shutting down");
        graph.shutdown();
      }
    }
  }

  private void dbClient2() {
    synchronized (LOCK) {
      OrientBaseGraph graph = new OrientGraph(CLIENT_ORIENT_URL_REPLICA/* SERVER_ORIENT_URL_REPLICA */);
      try {
        pause();

        // Let's give it some time for asynchronous replication.
        sleep(3000);

        OrientVertex v1 = graph.getVertex(vertex1Id);
        assertNotNull(v1);
        assertEquals(2, v1.getRecord().getVersion());
        assertEquals("v1", v1.getProperty("p1"));

        OrientVertex v2 = graph.getVertex(vertex2Id);
        assertNotNull(v2);
        assertEquals(2, v2.getRecord().getVersion());
        assertEquals("v2", v2.getProperty("p2"));
      } catch (Throwable e) {
        if (exceptionInThread == null) {
          exceptionInThread = e;
        }
      } finally {
        OLogManager.instance().info(this, "Shutting down");
        graph.shutdown();
        LOCK.notifyAll();
      }
    }
  }

  private static void sleep(int i) {
    try {
      Thread.sleep(i);
    } catch (InterruptedException xcpt) {
      xcpt.printStackTrace();
    }
  }

  private static void pause() {
    try {
      OLogManager.instance().info(null, "Waking up the neighbor");
      LOCK.notifyAll();
      OLogManager.instance().info(null, "Going to sleep");
      LOCK.wait();
      OLogManager.instance().info(null, "Awakening");
    } catch (InterruptedException xcpt) {
      xcpt.printStackTrace();
    }
  }

}

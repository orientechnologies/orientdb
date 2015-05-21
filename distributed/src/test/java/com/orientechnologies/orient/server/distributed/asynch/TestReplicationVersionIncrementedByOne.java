package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import junit.framework.TestCase;

import java.io.File;

public class TestReplicationVersionIncrementedByOne extends TestCase {

  private static final String CONFIG_DIR             = "src/test/resources";
  private static final String DB1_DIR                = "target/db1";
  private static final String SERVER_ORIENT_URL_MAIN = "plocal:" + DB1_DIR + "/databases/testDB";

  private volatile Throwable  exceptionInThread;

  public void testReplication2() throws Throwable {
    Orient.setRegisterDatabaseByPath(true);

    final BareBonesServer[] servers = new BareBonesServer[1];
    // Start the first DB server.
    Thread dbServer1 = new Thread() {
      @Override
      public void run() {
        servers[0] = dbServer(DB1_DIR, SERVER_ORIENT_URL_MAIN, "asynch-dserver-config-0.xml");
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

    servers[0].stop();
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
    OrientBaseGraph graph = new OrientGraph(SERVER_ORIENT_URL_MAIN);
    try {
      Vertex v1 = graph.addVertex("vertextype", (String) null);
      graph.commit();
      assertEquals(1, ((OrientVertex) v1).getRecord().getVersion());

      Vertex v2 = graph.addVertex("vertextype", (String) null);
      graph.commit();
      assertEquals(1, ((OrientVertex) v2).getRecord().getVersion());

      v1.addEdge("edgetype", v2);
      graph.commit();
      assertEquals(2, ((OrientVertex) v1).getRecord().getVersion());
      assertEquals(2, ((OrientVertex) v2).getRecord().getVersion());
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

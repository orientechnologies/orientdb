package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class TestAsyncReplMode2Servers2OpsCommit extends BareBoneBase2ServerTest {

  private Object vertex1Id;
  private Object vertex2Id;

  @Override
  protected String getDatabaseName() {
    return "TestAsyncReplMode2Servers2OpsCommit";
  }

  protected void dbClient1() {
    OGlobalConfiguration.LOG_CONSOLE_LEVEL.setValue("FINEST");

    synchronized (LOCK) {
      OrientBaseGraph graph = new OrientGraph(getLocalURL());
      try {
        OrientVertex vertex1 = graph.addVertex("vertextype", (String) null);
        graph.commit();
        assertEquals(1, vertex1.getRecord().getVersion());
        vertex1Id = vertex1.getId();

        OrientVertex vertex2 = graph.addVertex("vertextype", (String) null);
        vertex1.addEdge("edgetype", vertex2);
        graph.commit();
        assertEquals(2, vertex1.getRecord().getVersion());
        assertEquals(1, vertex2.getRecord().getVersion());
        vertex2Id = vertex2.getId();

        pause();
      } catch (Throwable e) {
        if (exceptionInThread == null) {
          exceptionInThread = e;
        }
      } finally {
        System.out.println("Shutting down");
        graph.shutdown();
        LOCK.notifyAll();
      }
    }
  }

  protected void dbClient2() {
    sleep(1000);

    synchronized (LOCK) {
      OrientBaseGraph graph = new OrientGraph(getLocalURL2());
      try {
        OrientVertex vertex1 = graph.getVertex(vertex1Id);
        assertEquals(2, vertex1.getRecord().getVersion());

        OrientVertex vertex2 = graph.getVertex(vertex2Id);
        assertEquals(1, vertex2.getRecord().getVersion());
      } catch (Throwable e) {
        if (exceptionInThread == null) {
          exceptionInThread = e;
        }
      } finally {
        System.out.println("Shutting down");
        graph.shutdown();
        LOCK.notifyAll();
      }
    }
  }

}

package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class TestAsyncReplMode2ServersAddEdge extends BareBoneBase2ServerTest {

  private static final int NUM_OF_LOOP_ITERATIONS = 100;

  private Object           parentV1Id;

  @Override
  protected String getDatabaseName() {
    return "TestAsyncReplMode2ServersAddEdge";
  }

  protected void dbClient1() {
    OGlobalConfiguration.LOG_CONSOLE_LEVEL.setValue("FINEST");

    synchronized (LOCK) {
      OrientBaseGraph graph = new OrientGraph(getLocalURL());
      try {
        OrientVertex parentV1 = graph.addVertex("vertextype", (String) null);
        graph.commit();
        assertEquals(1, parentV1.getRecord().getVersion());
        parentV1Id = parentV1.getId();

        for (int i = 0; i < NUM_OF_LOOP_ITERATIONS; i++) {
          Vertex childV = graph.addVertex("vertextype", (String) null);
          graph.commit();
          assertEquals(1, ((OrientVertex) childV).getRecord().getVersion());

          parentV1.addEdge("edgetype", childV);
          graph.commit();

          OLogManager.instance().error(this, "parentV1 %s v%d should be v%d", parentV1.getIdentity(),
              parentV1.getRecord().getVersion(), i + 2);

          assertEquals(i + 2, ((OrientVertex) parentV1).getRecord().getVersion());
          assertEquals(2, ((OrientVertex) childV).getRecord().getVersion());
        }

        pause();
      } catch (Throwable e) {
        OLogManager.instance().error(this, "Exception", e);
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
        sleep(5000);
        OrientVertex parentV1 = graph.getVertex(parentV1Id);
        assertEquals(NUM_OF_LOOP_ITERATIONS + 1, parentV1.getRecord().getVersion());
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

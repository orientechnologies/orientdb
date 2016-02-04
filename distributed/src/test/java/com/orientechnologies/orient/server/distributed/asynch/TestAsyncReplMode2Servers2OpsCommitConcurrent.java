package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.id.ORID;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.junit.Assert;

import java.util.concurrent.CountDownLatch;

public class TestAsyncReplMode2Servers2OpsCommitConcurrent extends BareBoneBase2ServerTest {

  private static final int TOTAL   = 100;
  private ORID             vertex1Id;
  CountDownLatch           counter = new CountDownLatch(2);

  @Override
  protected String getDatabaseName() {
    return "TestAsyncReplMode2Servers2OpsCommitConcurrent";
  }

  protected void dbClient1() {
    // OGlobalConfiguration.LOG_CONSOLE_LEVEL.setValue("FINEST");

    OrientBaseGraph graph = new OrientGraph(getLocalURL());
    OrientVertex vertex1 = graph.addVertex("vertextype", (String) null);
    graph.commit();
    graph.shutdown();

    vertex1Id = vertex1.getIdentity();

    exec("client1");
  }

  protected void dbClient2() {
    exec("client2");
  }

  protected void exec(final String iClient) {
    counter.countDown();

    try {
      counter.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    OrientBaseGraph graph = new OrientGraph(getLocalURL());

    OrientVertex vertex1 = graph.getVertex(vertex1Id);

    try {
      int i = 0;
      for (; i < TOTAL; ++i) {

        for (int retry = 0; retry < 20; ++retry) {
          try {
            OrientVertex vertex2 = graph.addVertex("vertextype", (String) null);
            vertex1.addEdge("edgetype", vertex2);
            graph.commit();

            System.out.println(iClient + " - successfully committed version: " + vertex1.getRecord().getVersion());
          } catch (ONeedRetryException e) {
            System.out.println(iClient + " - caught conflict, reloading vertex. v=" + vertex1.getRecord().getVersion());
            vertex1.reload();
          }
        }
      }

      // STATISTICALLY HERE AT LEAST ON CONFLICT HAS BEEN RECEIVED
      vertex1.reload();

      Assert.assertTrue(vertex1.getRecord().getVersion() > TOTAL * 2 + 1);
      Assert.assertEquals(TOTAL, i);

    } catch (Throwable e) {
      if (exceptionInThread == null)
        exceptionInThread = e;

    } finally {
      System.out.println("Shutting down");
      graph.shutdown();

      sleep(1000);
    }
  }
}

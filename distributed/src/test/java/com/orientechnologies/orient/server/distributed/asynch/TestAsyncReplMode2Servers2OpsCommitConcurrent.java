package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORID;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.junit.Assert;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class TestAsyncReplMode2Servers2OpsCommitConcurrent extends BareBoneBase2ServerTest {

  private static final int TOTAL = 1000;
  private ORID vertex1Id;
  CountDownLatch counter = new CountDownLatch(2);

  @Override
  protected String getDatabaseName() {
    return "TestAsyncReplMode2Servers2OpsCommitConcurrent";
  }

  protected void dbClient1() {
    OGlobalConfiguration.DISTRIBUTED_CONCURRENT_TX_AUTORETRY_DELAY.setValue(1);

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

    final AtomicInteger conflicts = new AtomicInteger(0);

    try {
      int i = 0;
      for (; i < TOTAL; ++i) {

        for (int retry = 0; retry < 20; ++retry) {
          try {
            OrientVertex vertex2 = graph.addVertex("vertextype", (String) null);
            vertex1.addEdge("edgetype", vertex2);
            graph.commit();
            break;

          } catch (ONeedRetryException e) {
            System.out.println(
                iClient + " - caught conflict, reloading vertex. v=" + vertex1.getRecord().getVersion() + " retry: " + retry
                    + " conflicts so far: " + conflicts.get());
            graph.rollback();
            vertex1.reload();

            conflicts.incrementAndGet();
          }
        }
      }

      // STATISTICALLY HERE AT LEAST ONE CONFLICT HAS BEEN RECEIVED
      vertex1.reload();

      Assert.assertTrue("No conflicts recorded. conflicts=" + conflicts.get(), conflicts.get() > 0);
      Assert.assertEquals(TOTAL, i);

    } catch (Throwable e) {
      if (exceptionInThread == null)
        exceptionInThread = e;

    } finally {
      System.out.println("Shutting down");
      graph.shutdown();
    }
  }
}

package org.apache.tinkerpop.gremlin.orientdb;

import java.util.ArrayList;
import java.util.List;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 31/07/2017. */
public class OrientSpecBehavioursTest extends OrientGraphBaseTest {

  @Test
  public void shouldNotFoundVertex() {

    OrientGraph tx = factory.getTx();

    Assert.assertEquals(0, StreamUtils.asStream(tx.vertices("#3:999")).count());

    tx.close();
  }

  @Test
  public void testExecuteWithRetryInDirtyTx() {
    OrientGraph graph = factory.getTx();
    graph.addVertex();
    try {
      graph.executeWithRetry(2, (db) -> null);
      Assert.fail();
    } catch (IllegalStateException x) {
    }
    graph.tx().rollback();
  }

  @Test
  public void testExecuteWithRetryWrongN() {
    OrientGraph graph = factory.getTx();
    try {
      graph.executeWithRetry(-1, (db) -> null);
      Assert.fail();
    } catch (IllegalArgumentException x) {
    }
  }

  @Test
  public void testExecuteWithRetryTxStatus() {

    OrientGraph graph = factory.getTx();

    graph.executeWithRetry(1, (db) -> null);
    Assert.assertFalse(graph.tx().isOpen());

    graph.tx().open();

    graph.executeWithRetry(1, (db) -> null);
    Assert.assertTrue(graph.tx().isOpen());
    graph.tx().rollback();
  }

  @Test
  public void testExecuteWithRetry() throws InterruptedException {
    String className = "testExecuteWithRetry";

    OrientGraph noTx = factory.getNoTx();
    noTx.createClass(className, "V");

    OrientVertex v = (OrientVertex) noTx.addVertex(T.label, className, "count", 0);

    int nThreads = 4;
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < nThreads; i++) {
      Thread thread =
          new Thread(
              () -> {
                OrientGraph graph = factory.getTx();
                graph.executeWithRetry(
                    10,
                    (db) -> {
                      Vertex vertex = graph.vertices(v.getIdentity()).next();
                      try {
                        Thread.sleep(1000);
                      } catch (InterruptedException e) {
                      }
                      int count = (int) vertex.property("count").value();
                      vertex.property("count", count + 1);
                      return vertex;
                    });
                graph.close();
              });
      threads.add(thread);
      thread.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    v.getRawElement().reload();
    Assert.assertEquals(nThreads, v.property("count").value());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testRetryNotSupportedException() {

    OrientGraph noTx = factory.getNoTx();

    try {
      noTx.executeWithRetry(10, (graph) -> null);
    } finally {
      noTx.close();
    }
  }

  @Test
  public void testExecuteWithRetryTx() throws InterruptedException {
    String className = "testExecuteWithRetryTx";
    OrientGraph noTx = factory.getNoTx();
    noTx.createClass(className, "V");

    OrientVertex v = (OrientVertex) noTx.addVertex(T.label, className, "count", 0);
    int nThreads = 4;
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < nThreads; i++) {
      Thread thread =
          new Thread() {
            @Override
            public void run() {
              OrientGraph graph = factory.getTx();

              graph.tx().open();
              graph.executeWithRetry(
                  10,
                  (db) -> {
                    Vertex vertex = graph.vertices(v.getIdentity()).next();
                    try {
                      Thread.sleep(1000);
                    } catch (InterruptedException e) {
                    }
                    int count = (int) vertex.property("count").value();
                    vertex.property("count", count + 1);
                    return vertex;
                  });
              graph.tx().commit();
              graph.close();
            }
          };
      threads.add(thread);
      thread.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    v.getRawElement().reload();
    Assert.assertEquals(nThreads, v.property("count").value());
  }
}

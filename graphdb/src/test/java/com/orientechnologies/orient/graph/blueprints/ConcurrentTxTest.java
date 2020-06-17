package com.orientechnologies.orient.graph.blueprints;

import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConcurrentTxTest {

  private static final String STORAGE_ENGINE = "memory";
  private static final String DATABASE_URL =
      STORAGE_ENGINE + ":" + ConcurrentTxTest.class.getSimpleName();

  private static final String PROPERTY_NAME = "pn";
  OrientGraphFactory graphFactory;

  @Before
  public void setUpGraph() {
    graphFactory = new OrientGraphFactory(DATABASE_URL);
    graphFactory.setAutoStartTx(false);
    graphFactory.setupPool(0, 200);
  }

  @After
  public void tearDownGraph() {
    graphFactory.drop();
  }

  @Test(expected = OConcurrentModificationException.class)
  public void testMultithreadedProvokeOConcurrentModificationException2() throws Throwable {
    // Create vertex
    OrientGraph mainTx = graphFactory.getTx();
    mainTx.begin();
    OrientVertex vertex = mainTx.addVertex(null, PROPERTY_NAME, "init");
    mainTx.commit();
    mainTx.shutdown();

    int threadCount = 200;
    final Object recordId = vertex.getId();
    final CyclicBarrier barrier = new CyclicBarrier(threadCount);
    List<Thread> threads = new ArrayList<Thread>();
    final AtomicReference<Throwable> t = new AtomicReference<Throwable>(null);

    // Spawn two threads and modify the vertex
    for (int i = 0; i < threadCount; i++) {
      final int threadNo = i;
      Thread thread =
          run(
              new Runnable() {

                @Override
                public void run() {
                  OrientGraph tx = graphFactory.getTx();
                  try {
                    tx.begin();
                    OrientVertex secondVertexHandle = tx.getVertex(recordId);
                    secondVertexHandle.setProperty(PROPERTY_NAME, threadNo);
                    waitFor(barrier);
                    tx.commit();
                  } catch (Exception e) {
                    t.set(e);
                  } finally {
                    tx.shutdown();
                  }
                }
              });
      threads.add(thread);
    }

    // Wait for threads
    for (Thread thread : threads) {
      thread.join();
    }
    if (t.get() != null) {
      throw t.get();
    }
  }

  @Test(expected = OConcurrentModificationException.class)
  public void testMultithreadedProvokeOConcurrentModificationException() throws Throwable {

    final int firstValue = 0;
    final int secondValue = 1;

    // Create vertex
    OrientGraph mainTx = graphFactory.getTx();
    mainTx.begin();
    OrientVertex firstVertexHandle = mainTx.addVertex(null, PROPERTY_NAME, firstValue);
    mainTx.commit();
    mainTx.shutdown();

    final Object recordId = firstVertexHandle.getId();
    final CyclicBarrier barrier = new CyclicBarrier(2);
    List<Thread> threads = new ArrayList<Thread>();
    final AtomicReference<Throwable> t = new AtomicReference<Throwable>(null);

    // Spawn two threads and modify the vertex
    for (int i = 0; i < 2; i++) {
      Thread thread =
          run(
              new Runnable() {

                @Override
                public void run() {
                  OrientGraph tx = graphFactory.getTx();
                  try {
                    tx.begin();
                    Vertex secondVertexHandle = tx.getVertex(recordId);
                    secondVertexHandle.setProperty(PROPERTY_NAME, secondValue);
                    waitFor(barrier);
                    tx.commit();
                  } catch (Exception e) {
                    t.set(e);
                  } finally {
                    tx.shutdown();
                  }
                }
              });

      threads.add(thread);
    }

    // Wait for threads
    for (Thread thread : threads) {
      thread.join();
    }
    if (t.get() != null) {
      throw t.get();
    }
  }

  private void waitFor(CyclicBarrier barrier) {
    try {
      barrier.await(1000, TimeUnit.SECONDS);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test(expected = OConcurrentModificationException.class)
  public void testProvokeOConcurrentModificationException() throws Exception {

    final int firstValue = 0;
    final int secondValue = 1;
    final int thirdValue = 3;

    // Create vertex
    OrientGraph tx = graphFactory.getTx();
    tx.begin();
    final OrientVertex firstVertexHandle = tx.addVertex(null, PROPERTY_NAME, firstValue);
    tx.commit();
    // tx.shutdown();

    final Object recordId = firstVertexHandle.getId();

    Thread updateThread =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                // 1. Update
                OrientGraph tx2 = graphFactory.getTx();
                try {
                  tx2.begin();
                  Vertex secondVertexHandle = tx2.getVertex(recordId);
                  secondVertexHandle.setProperty(PROPERTY_NAME, secondValue);
                  tx2.commit();
                } finally {
                  tx2.shutdown();
                }
              }
            });
    updateThread.start();
    updateThread.join();

    // 2. Update
    OrientGraph tx3 = graphFactory.getTx();
    try {
      tx3.begin();
      Vertex thirdVertexHandle = tx3.getVertex(recordId);
      thirdVertexHandle.setProperty(PROPERTY_NAME, thirdValue);

      // commit
      tx3.commit();
    } finally {
      tx3.shutdown();
      tx.shutdown();
    }
  }

  public static Thread run(Runnable runnable) {
    Thread thread = new Thread(runnable);
    thread.start();
    return thread;
  }
}

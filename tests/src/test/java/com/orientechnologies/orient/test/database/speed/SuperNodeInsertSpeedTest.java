package com.orientechnologies.orient.test.database.speed;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.orientechnologies.orient.core.id.ORID;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class SuperNodeInsertSpeedTest {

  private static final int   WARM_UP_COUNT = 30000;
  private static final int   TEST_COUNT    = 50000;
  private static final int   THREAD_COUNT  = 1;
  private static OrientGraph graph;

  private static void setUp() {
    graph = new OrientGraph("plocal:target/SuperNodeInsertSpeedTest");
  }

  private static void tearDown() {
    graph.shutdown();
  }

  public static void main(String[] args) throws InterruptedException {
    setUp();

    System.out.println("Test insert super-node");

    final long time = test();

    printStats(time);

    tearDown();
  }

  private static long test() throws InterruptedException {
    OrientVertex superNode = graph.addVertex("");
    superNode.save();
    graph.commit();
    final ORID superNodeId = superNode.getIdentity();

    // warm up
    createLinks(WARM_UP_COUNT, superNode);

    final CountDownLatch startLatch = new CountDownLatch(THREAD_COUNT);
    final CountDownLatch endLatch = new CountDownLatch(THREAD_COUNT);

    final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
    for (int i = 0; i < THREAD_COUNT; i++) {
      executorService.submit(new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          startLatch.countDown();
          startLatch.await();

          long time = doTest(superNodeId);

          endLatch.countDown();

          return time;
        }
      });
    }

    startLatch.await();
    final long startTime = System.currentTimeMillis();

    endLatch.await();
    final long time = System.currentTimeMillis() - startTime;

    return time;
  }

  private static long doTest(ORID superNodeId) {
    OrientVertex superNode = graph.getVertex(superNodeId);

    final long startTime = System.currentTimeMillis();
    createLinks(TEST_COUNT / THREAD_COUNT, superNode);
    return System.currentTimeMillis() - startTime;
  }

  private static void printStats(long time) {
    System.out.println("Test summary");
    System.out.println("============");
    System.out.println("Thread count: " + THREAD_COUNT);
    System.out.println("Time:         " + time + "ms");
    System.out.println("Bandwidth:    " + ((double) TEST_COUNT) / time * 1000 + " it/sec");
  }

  private static void createLinks(int vertexNumber, OrientVertex superNode) {
    for (int i = 0; i < vertexNumber; i++) {
      final OrientVertex v = graph.addVertex("");
      v.addEdge("link", superNode);
      v.save();
      graph.commit();
    }
  }
}

package com.orientechnologies.orient.test.database.speed;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class SuperNodeInsertSpeedTest {

  private static final int    WARM_UP_COUNT = 50000;
  private static final int    TEST_COUNT    = 60000;
  private static final int    THREAD_COUNT  = 5;
  private final int           threadCount;
  private OrientGraph         graph;
  private final AtomicInteger retryCount    = new AtomicInteger();

  public SuperNodeInsertSpeedTest(int threadCount1) {
    threadCount = threadCount1;
  }

  private void setUp() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);

    graph = openGraph();
  }

  private OrientGraph openGraph() {
    return new OrientGraph("plocal:target/SuperNodeInsertSpeedTest");
  }

  private void tearDown() {
    graph.shutdown();
  }

  public static void main(String[] args) throws InterruptedException {
    System.out.println("Test insert super-node");

    for (int threadCount = 1; threadCount <= THREAD_COUNT; threadCount++) {

      List<Double> results = new ArrayList<Double>();
      for (int i = 0; i < 20; i++) {
        final double time = new SuperNodeInsertSpeedTest(threadCount).start();

        results.add(time);
      }
      System.out.println();
      System.out.println("Thread count: " + threadCount);
      stat(results);
    }
  }

  private static void stat(List<Double> results) {
    System.out.println("Performance: " + avg(results) + "(" + sd(results) + ")");
    System.out.println("Median:      " + median(results));
    System.out.println();
    System.out.println();
  }

  private static double sd(List<Double> results) {
    final double avg = avg(results);
    final int n = results.size();
    double sum = 0;
    for (Double result : results) {
      sum += sqr(result - avg) / n;
    }

    return Math.sqrt(sum);
  }

  private static double sqr(Double result) {
    return result * result;
  }

  private static double avg(List<Double> results) {
    final double sum = sum(results);
    final int n = results.size();

    return sum / n;
  }

  private static Double median(List<Double> results) {
    Collections.sort(results);

    final int n = results.size();
    if (n % 2 == 0) {
      return (results.get(n / 2) + results.get(n / 2 - 1)) / 2;
    } else
      return (results.get(n / 2));
  }

  private static double sum(List<Double> results) {
    double sum = 0;
    for (Double result : results) {
      sum += result;
    }
    return sum;
  }

  public double start() throws InterruptedException {
    setUp();

    System.out.print("#");

    final long time = test();

    tearDown();

    return ((double) TEST_COUNT) / time * 1000;
  }

  private long test() throws InterruptedException {
    OrientVertex superNode = graph.addVertex("");
    superNode.save();
    graph.commit();
    final ORID superNodeId = superNode.getIdentity();

    // warm up
    createLinks(WARM_UP_COUNT, superNode, graph);

    final CountDownLatch startLatch = new CountDownLatch(threadCount);
    final CountDownLatch endLatch = new CountDownLatch(threadCount);

    final ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    for (int i = 0; i < threadCount; i++) {
      executorService.submit(new Callable<Long>() {
        @Override
        public Long call() throws Exception {
          final OrientGraph graph = openGraph();
          OrientVertex superNode = graph.getVertex(superNodeId);

          startLatch.countDown();
          startLatch.await();

          long time = doTest(superNode, graph);

          endLatch.countDown();

          graph.shutdown();

          return time;
        }
      });
    }

    startLatch.await();
    final long startTime = System.currentTimeMillis();

    endLatch.await();
    final long time = System.currentTimeMillis() - startTime;

    executorService.shutdown();

    return time;
  }

  private long doTest(OrientVertex superNode, OrientGraph graph) {
    final long startTime = System.currentTimeMillis();
    createLinks(TEST_COUNT / threadCount, superNode, graph);
    return System.currentTimeMillis() - startTime;
  }

  private void createLinks(int vertexNumber, OrientVertex superNode, OrientGraph graph) {
    for (int i = 0; i < vertexNumber; i++) {
      while (true)
        try {
          final OrientVertex v = graph.addVertex("");
          v.addEdge("link", superNode);
          v.save();
          graph.commit();
          break;
        } catch (OConcurrentModificationException e) {
          superNode.getRecord().reload();
          retryCount.incrementAndGet();
        }
    }
  }
}

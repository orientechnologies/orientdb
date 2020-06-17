package com.orientechnologies.orient.test.database.auto;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.orientechnologies.orient.core.conflict.OAutoMergeRecordConflictStrategy;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.testng.annotations.Test;

public class DatabaseConflictStrategyAutoMergeTest {

  private static final String CLIENT_ORIENT_URL_MAIN = "memory:testAutoMerge";

  private static final int NUM_OF_LOOP_ITERATIONS = 50;
  private static Object LOCK = new Object();
  private volatile Throwable exceptionInThread;
  private Object parentV1Id;
  private Object parentV2Id;

  @Test
  public void testAutoMerge() throws Throwable {
    OrientGraphFactory factory = new OrientGraphFactory(CLIENT_ORIENT_URL_MAIN);
    OrientBaseGraph graph = factory.getNoTx();
    graph.setConflictStrategy(OAutoMergeRecordConflictStrategy.NAME);
    graph.createVertexType("vertextype");
    graph.createEdgeType("edgetype");
    graph.shutdown();
    factory.close();

    Thread dbClient1 =
        new Thread() {
          @Override
          public void run() {
            dbClient1();
          }
        };

    dbClient1.start();
    // Start the second DB client.
    Thread dbClient2 =
        new Thread() {
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
  }

  private void dbClient1() {
    sleep(500);

    synchronized (LOCK) {
      OrientBaseGraph graph = new OrientGraph(CLIENT_ORIENT_URL_MAIN);
      try {
        // Create 2 parent vertices.
        OrientVertex parentV1 = graph.addVertex("vertextype1", (String) null);
        graph.commit();
        assertEquals(1, parentV1.getRecord().getVersion());
        parentV1Id = parentV1.getId();

        OrientVertex parentV2 = graph.addVertex("vertextype2", (String) null);
        graph.commit();
        assertEquals(1, parentV2.getRecord().getVersion());
        parentV2Id = parentV2.getId();

        // Create vertices.
        for (int i = 0; i < NUM_OF_LOOP_ITERATIONS; i++) {
          pause();

          if (exceptionInThread != null) break;
          OrientVertex vertex = graph.addVertex("vertextype3", (String) null);
          graph.commit();
          assertEquals(1, vertex.getRecord().getVersion());

          vertex.setProperty("num", i);
          graph.commit();
          assertEquals(2, vertex.getRecord().getVersion());

          parentV1.addEdge("edgetype1", vertex);
          graph.commit();
          assertNotNull(
              parentV1.getProperty("cnt"),
              "record " + parentV1.getIdentity() + " has no 'cnt' property");
          boolean edge1Exists = false;
          for (Edge e : parentV1.getEdges(Direction.OUT, "edgetype1")) {
            if (e.getVertex(Direction.IN).equals(vertex)) {
              edge1Exists = true;
              break;
            }
          }
          assertTrue(edge1Exists);
          boolean edge2Exists = false;
          for (Edge e : vertex.getEdges(Direction.IN, "edgetype1")) {
            if (e.getVertex(Direction.OUT).equals(parentV1)) {
              edge2Exists = true;
              break;
            }
          }
          assertTrue(edge2Exists);
          assertNotNull(vertex.getProperty("num"));

          parentV2.addEdge("edgetype2", vertex);
          graph.commit();
          assertNotNull(parentV2.getProperty("cnt"));
          edge1Exists = false;
          for (Edge e : parentV2.getEdges(Direction.OUT, "edgetype2")) {
            if (e.getVertex(Direction.IN).equals(vertex)) {
              edge1Exists = true;
              break;
            }
          }
          assertTrue(edge1Exists);
          edge2Exists = false;
          for (Edge e : vertex.getEdges(Direction.IN, "edgetype2")) {
            if (e.getVertex(Direction.OUT).equals(parentV2)) {
              edge2Exists = true;
              break;
            }
          }
          assertTrue(edge2Exists);
          assertNotNull(vertex.getProperty("num"));
        }
      } catch (Throwable e) {
        if (exceptionInThread == null) {
          exceptionInThread = e;
        }
      } finally {
        graph.shutdown();
        LOCK.notifyAll();
      }
    }
  }

  private void dbClient2() {
    synchronized (LOCK) {
      OrientBaseGraph graph = new OrientGraph(CLIENT_ORIENT_URL_MAIN);
      OrientVertex parentV1 = null;
      OrientVertex parentV2 = null;
      int countPropValue = 0;
      try {
        for (int i = 0; i < NUM_OF_LOOP_ITERATIONS; i++) {
          pause();

          if (exceptionInThread != null) break;
          countPropValue++;
          if (parentV1 == null) {
            parentV1 = graph.getVertex(parentV1Id);
          }
          parentV1.setProperty("cnt", countPropValue);
          graph.commit();

          if (parentV2 == null) {
            parentV2 = graph.getVertex(parentV2Id);
          }
          parentV2.setProperty("cnt", countPropValue);
          graph.commit();
        }
      } catch (Throwable e) {
        if (exceptionInThread == null) {
          exceptionInThread = e;
        }
      } finally {
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
      LOCK.notifyAll();
      LOCK.wait();
    } catch (InterruptedException xcpt) {
      xcpt.printStackTrace();
    }
  }
}

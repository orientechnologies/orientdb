package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.conflict.OAutoMergeRecordConflictStrategy;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.testng.annotations.Test;

import java.util.ArrayList;

import static org.testng.Assert.assertEquals;

public class DatabaseConflictStrategyAutoMergeTest {

  private static final String CLIENT_ORIENT_URL_MAIN = "memory:testAutoMerge";

  private static Object       LOCK                   = new Object();
  private ArrayList<Object>   verticeIds             = new ArrayList<Object>();
  private volatile Throwable  exceptionInThread;

  @Test
  public void testAutoMerge() throws Throwable {
    OrientGraphFactory factory = new OrientGraphFactory(CLIENT_ORIENT_URL_MAIN);
    OrientBaseGraph graph = factory.getNoTx();
    graph.setConflictStrategy(OAutoMergeRecordConflictStrategy.NAME);
    graph.createVertexType("vertextype");
    graph.createEdgeType("edgetype");
    graph.shutdown();
    factory.close();

    Thread dbClient1 = new Thread() {
      @Override
      public void run() {
        dbClient1();
      }
    };

    dbClient1.start();
    // Start the second DB client.
    Thread dbClient2 = new Thread() {
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
    sleep(1000);

    synchronized (LOCK) {
      OrientBaseGraph graph = new OrientGraph(CLIENT_ORIENT_URL_MAIN);
      ArrayList<OrientVertex> cachedVertices = new ArrayList<OrientVertex>();
      try {
        // Create vertices.
        for (int i = 0; i < 10; i++) {
          OrientVertex v = graph.addVertex("vertextype", (String) null);
          graph.commit();
          assertEquals(1, v.getRecord().getVersion());
          verticeIds.add(v.getId());
          cachedVertices.add(v);
        }
        pause();

        for (OrientVertex cachedVertex : cachedVertices) {
          cachedVertex.setProperty("p2", "v2");
          graph.commit();

          cachedVertex.reload();
          assertEquals("v1", cachedVertex.getProperty("p1"));
          assertEquals("v2", cachedVertex.getProperty("p2"));
          assertEquals(3, cachedVertex.getRecord().getVersion());
        }
        /*
         * pause();
         * 
         * OrientVertex previousVertex = null; for (OrientVertex cachedVertex: cachedVertices) { if (previousVertex == null) {
         * previousVertex = cachedVertex; } else { OrientEdge e = (OrientEdge) previousVertex.addEdge("edgetype", cachedVertex);
         * graph.commit(); assertEquals(5, previousVertex.getRecord().getVersion()); assertEquals(5,
         * cachedVertex.getRecord().getVersion()); assertEquals("v1", previousVertex.getProperty("p1")); assertEquals("v2",
         * previousVertex.getProperty("p2")); assertTrue(previousVertex.getEdges(Direction.OUT, "edgetype").iterator().hasNext());
         * assertTrue(cachedVertex.getEdges(Direction.IN, "edgetype").iterator().hasNext()); assertEquals("v1",
         * cachedVertex.getProperty("p1")); assertEquals("v2", cachedVertex.getProperty("p2")); assertEquals(1,
         * e.getRecord().getVersion()); } }
         */
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
      ArrayList<OrientVertex> cachedVertices = new ArrayList<OrientVertex>();
      try {
        pause();

        for (int i = 0; i < verticeIds.size(); i++) {
          OrientVertex v = graph.getVertex(verticeIds.get(i));
          assertEquals(1, v.getRecord().getVersion());
          cachedVertices.add(v);

          v.setProperty("p1", "v1");
          graph.commit();
          assertEquals(2, v.getRecord().getVersion());
        }
        pause();

        /*
         * OrientVertex previousVertex = null; for (OrientVertex cachedVertex: cachedVertices) { if (previousVertex == null) {
         * previousVertex = cachedVertex; } else { OrientEdge e = (OrientEdge) previousVertex.addEdge("edgetype", cachedVertex);
         * graph.commit(); assertEquals(4, previousVertex.getRecord().getVersion()); assertEquals(4,
         * cachedVertex.getRecord().getVersion()); assertEquals("v1", previousVertex.getProperty("p1")); assertEquals("v2",
         * previousVertex.getProperty("p2")); assertTrue(previousVertex.getEdges(Direction.OUT, "edgetype").iterator().hasNext());
         * assertTrue(cachedVertex.getEdges(Direction.IN, "edgetype").iterator().hasNext()); assertEquals("v1",
         * cachedVertex.getProperty("p1")); assertEquals("v2", cachedVertex.getProperty("p2")); assertEquals(1,
         * e.getRecord().getVersion()); previousVertex = cachedVertex; } } pause();
         */
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
